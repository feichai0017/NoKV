package benchmark

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func newEtcdEngine(opts ycsbEngineOptions) ycsbEngine {
	return &etcdEngine{opts: opts}
}

type etcdEngine struct {
	opts ycsbEngineOptions
	srv  *embed.Etcd
	cli  *clientv3.Client
	dir  string
}

func (e *etcdEngine) Name() string { return "etcd" }

func (e *etcdEngine) Open(clean bool) error {
	dir := e.opts.engineDir("etcd")
	if clean {
		if err := ensureCleanDir(dir); err != nil {
			return fmt.Errorf("etcd ensure dir: %w", err)
		}
	} else if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("etcd mkdir dir: %w", err)
	}

	cfg := embed.NewConfig()
	cfg.Dir = dir
	cfg.LogLevel = "error"
	cfg.EnableGRPCGateway = false
	cfg.LogOutputs = []string{filepath.Join(dir, "etcd.log")}

	lc, err := url.Parse("http://127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("parse etcd client url: %w", err)
	}
	lp, err := url.Parse("http://127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("parse etcd peer url: %w", err)
	}
	cfg.ListenClientUrls = []url.URL{*lc}
	cfg.AdvertiseClientUrls = []url.URL{*lc}
	cfg.ListenPeerUrls = []url.URL{*lp}
	cfg.AdvertisePeerUrls = []url.URL{*lp}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)

	srv, err := embed.StartEtcd(cfg)
	if err != nil {
		return fmt.Errorf("start embedded etcd: %w", err)
	}

	select {
	case <-srv.Server.ReadyNotify():
	case err := <-srv.Err():
		return fmt.Errorf("embedded etcd error: %w", err)
	case <-time.After(30 * time.Second):
		srv.Server.Stop()
		return fmt.Errorf("embedded etcd start timeout")
	}

	if len(srv.Clients) == 0 {
		srv.Close()
		return fmt.Errorf("embedded etcd has no client listeners")
	}
	endpoint := srv.Clients[0].Addr().String()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		srv.Close()
		return fmt.Errorf("create etcd client: %w", err)
	}

	e.dir = dir
	e.srv = srv
	e.cli = cli
	return nil
}

func (e *etcdEngine) Close() error {
	if e == nil {
		return nil
	}
	if e.cli != nil {
		_ = e.cli.Close()
	}
	if e.srv != nil {
		e.srv.Close()
		select {
		case <-e.srv.Server.StopNotify():
		case <-time.After(5 * time.Second):
		}
	}
	return nil
}

func (e *etcdEngine) Read(key []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := e.cli.Get(ctx, string(key))
	return err
}

func (e *etcdEngine) Insert(key, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := e.cli.Put(ctx, string(key), string(value))
	return err
}

func (e *etcdEngine) Update(key, value []byte) error {
	return e.Insert(key, value)
}

func (e *etcdEngine) Scan(startKey []byte, count int) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := e.cli.Get(ctx, string(startKey), clientv3.WithFromKey(), clientv3.WithLimit(int64(count)))
	if err != nil {
		return 0, err
	}
	return len(resp.Kvs), nil
}
