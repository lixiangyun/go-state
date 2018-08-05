package broker

import (
	"context"
	"log"
	"time"

	"github.com/coreos/etcd/clientv3"
)

type EtcdConn struct {
	config clientv3.Config
	client *clientv3.Client
	cancel context.CancelFunc
}

func NewEtcdClient(endpoints []string, timeout time.Duration) *EtcdConn {
	var err error
	etcdconn := new(EtcdConn)

	etcdconn.config.DialTimeout = timeout
	etcdconn.config.Endpoints = endpoints
	etcdconn.config.Context, etcdconn.cancel = context.WithCancel(context.Background())

	etcdconn.client, err = clientv3.New(etcdconn.config)
	if err != nil {
		log.Println(err.Error())
		return nil
	}

	return etcdconn
}

func (e *EtcdConn) Close() {
	e.cancel()
	e.client.Close()
}
