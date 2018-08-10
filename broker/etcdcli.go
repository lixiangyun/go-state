package broker

import (
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
)

type EtcdConn struct {
	config clientv3.Config
	client *clientv3.Client
	cancel context.CancelFunc
}

func NewEtcdClient(endpoints []string, timeout time.Duration) (*EtcdConn, error) {
	var err error
	etcdconn := new(EtcdConn)

	etcdconn.config.DialTimeout = timeout
	etcdconn.config.Endpoints = endpoints
	etcdconn.config.Context, etcdconn.cancel = context.WithCancel(context.Background())

	etcdconn.client, err = clientv3.New(etcdconn.config)
	if err != nil {
		return nil, err
	}

	return etcdconn, nil
}

func (e *EtcdConn) Call() *clientv3.Client {
	return e.client
}

func (e *EtcdConn) Close() {
	e.cancel()
	e.client.Close()
}
