package main

import (
	"context"
	"github.com/filecoin-project/lotus/storage"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	mux "github.com/gorilla/mux"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr-net"
	"golang.org/x/xerrors"
	"gopkg.in/urfave/cli.v2"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/apistruct"
	"github.com/filecoin-project/lotus/build"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/lib/auth"
	"github.com/filecoin-project/lotus/lib/jsonrpc"
	"github.com/filecoin-project/lotus/node"
	"github.com/filecoin-project/lotus/node/impl"
	"github.com/filecoin-project/lotus/node/repo"
)

var runCmd = &cli.Command{
	Name:  "run",
	Usage: "Start a lotus storage miner process",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "api",
			Value: "2345",
		},
		&cli.BoolFlag{
			Name:  "enable-gpu-proving",
			Usage: "enable use of GPU for mining operations",
			Value: true,
		},
		&cli.BoolFlag{
			Name:  "nosync",
			Usage: "don't check full-node sync status",
		},
	},
	Action: func(cctx *cli.Context) error {
		if !cctx.Bool("enable-gpu-proving") {
			os.Setenv("BELLMAN_NO_GPU", "true")
		}

		nodeApi, ncloser, err := lcli.GetFullNodeAPI(cctx)
		if err != nil {
			return err
		}
		defer ncloser()
		ctx := lcli.DaemonContext(cctx)

		v, err := nodeApi.Version(ctx)
		if err != nil {
			return err
		}

		if v.APIVersion != build.APIVersion {
			return xerrors.Errorf("lotus-daemon API version doesn't match: local: %s", api.Version{APIVersion: build.APIVersion})
		}

		log.Info("Checking full node sync status")

		if !cctx.Bool("nosync") {
			if err := lcli.SyncWait(ctx, nodeApi); err != nil {
				return xerrors.Errorf("sync wait: %w", err)
			}
		}

		storageRepoPath := cctx.String(FlagStorageRepo)
		r, err := repo.NewFS(storageRepoPath)
		if err != nil {
			return err
		}

		ok, err := r.Exists()
		if err != nil {
			return err
		}
		if !ok {
			return xerrors.Errorf("repo at '%s' is not initialized, run 'lotus-storage-miner init' to set it up", storageRepoPath)
		}

		var minerapi api.StorageMiner
		stop, err := node.New(ctx,
			node.StorageMiner(&minerapi),
			node.Online(),
			node.Repo(r),

			node.ApplyIf(func(s *node.Settings) bool { return cctx.IsSet("api") },
				node.Override(node.SetApiEndpointKey, func(lr repo.LockedRepo) error {
					apima, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/" +
						cctx.String("api"))
					if err != nil {
						return err
					}
					return lr.SetAPIEndpoint(apima)
				})),
			node.Override(new(api.FullNode), nodeApi),
		)
		if err != nil {
			return err
		}

		endpoint, err := r.APIEndpoint()
		if err != nil {
			return err
		}

		// Bootstrap with full node
		remoteAddrs, err := nodeApi.NetAddrsListen(ctx)
		if err != nil {
			return err
		}

		if err := minerapi.NetConnect(ctx, remoteAddrs); err != nil {
			return err
		}

		log.Infof("Remote version %s", v)

		lst, err := manet.Listen(endpoint)
		if err != nil {
			return xerrors.Errorf("could not listen: %w", err)
		}

		mux := mux.NewRouter()

		rpcServer := jsonrpc.NewServer()
		rpcServer.Register("Filecoin", apistruct.PermissionedStorMinerAPI(minerapi))

		mux.Handle("/rpc/v0", rpcServer)
		mux.PathPrefix("/remote").HandlerFunc(minerapi.(*impl.StorageMinerAPI).ServeRemote)
		mux.PathPrefix("/").Handler(http.DefaultServeMux) // pprof

		ah := &auth.Handler{
			Verify: minerapi.AuthVerify,
			Next:   mux.ServeHTTP,
		}

		srv := &http.Server{Handler: ah}

		sigChan := make(chan os.Signal, 2)
		go func() {
			<-sigChan
			log.Warn("Shutting down..")
			if err := stop(context.TODO()); err != nil {
				log.Errorf("graceful shutting down failed: %s", err)
			}
			if err := srv.Shutdown(context.TODO()); err != nil {
				log.Errorf("shutting down RPC server failed: %s", err)
			}
			log.Warn("Graceful shutdown successful")
		}()
		signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

		go func() {
			log.Infof("Begin pledge sector")
			ctx := lcli.ReqContext(cctx)
			for {
				pledgeMode := storage.PledgeSectorMode(minerapi.GetPledgeSectorMode(ctx))
				log.Infof("pledge sector mode: %s", pledgeMode)
				if pledgeMode == storage.PledgeSectorModeClose {
					select {
					case <-ctx.Done():
						log.Infof("End pledge sector")
						return
					case <-time.After(build.BlockDelay * time.Second):
					}
					continue
				}

				wstat, err := minerapi.WorkerStats(ctx)
				if err != nil {
					log.Errorf("Pledge: WorkerStats fail: %w", err)
					return
				}
				log.Infof("Pledge: Local %d / %d (+%d reserved)", wstat.LocalTotal-wstat.LocalReserved-wstat.LocalFree, wstat.LocalTotal-wstat.LocalReserved, wstat.LocalReserved)
				log.Infof("Pledge: Remote %d / %d", wstat.RemotesTotal-wstat.RemotesFree, wstat.RemotesTotal)
				threshold := 0
				if pledgeMode == storage.PledgeSectorModeAll {
					threshold = wstat.LocalFree + wstat.RemotesFree
				} else if pledgeMode == storage.PledgeSectorModeRemote {
					threshold = wstat.RemotesFree
				} else if pledgeMode == storage.PledgeSectorModeLocal {
					threshold = wstat.LocalFree
				} else {
					log.Errorf("Unknown pledge sector mode: %s", pledgeMode)
					return
				}
				log.Infof("Pledge: threshold %d", threshold)
				wg := sync.WaitGroup{}
				wg.Add(threshold)
				for i := 0; i < threshold; i++ {
					go func() {
						err := minerapi.PledgeSector(ctx)
						if err != nil {
							log.Errorf("Pledge sector error: %w", err)
						} else {
							log.Infof("Success pledge sector")
						}
						wg.Done()
					}()
				}
				wg.Wait()
				if threshold > 0 {
					continue
				}
				select {
				case <-ctx.Done():
					log.Infof("End pledge sector")
					return
				case <-time.After(build.BlockDelay * time.Second):
				}
			}
		}()

		return srv.Serve(manet.NetListener(lst))
	},
}
