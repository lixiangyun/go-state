package broker

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/coreos/etcd/clientv3"

	mvcc "github.com/coreos/etcd/mvcc/mvccpb"
)

const (
	defaultTTL      = 5
	defaultTimeout  = 3 * time.Second
	defaultTimes    = 3
	defaultTryTimes = 3
)

type EVENT_TYPE int

const (
	_ EVENT_TYPE = iota
	EVENT_ADD
	EVENT_UPDATE
	EVENT_DELETE
	EVENT_EXPIRE
)

type KeyValue struct {
	Key   string
	Value string
}

type KvWatchRsq struct {
	Act   EVENT_TYPE
	Key   string
	Value string
}

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

func (e *EtcdConn) Put(key string, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	_, err := e.client.Put(ctx, key, string(value))
	cancel()
	if err != nil {
		return err
	}
	return nil
}

func (e *EtcdConn) PutTTL(key string, value []byte, ttl int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	resp, err := e.client.Grant(ctx, ttl)
	cancel()
	if err != nil {
		return err
	}

	ctx, cancel = context.WithTimeout(context.Background(), defaultTimeout)
	_, err = e.client.Put(ctx, key, string(value), clientv3.WithLease(resp.ID))
	cancel()
	if err != nil {
		return err
	}
	return nil
}

func (e *EtcdConn) Get(key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	resp, err := e.client.Get(ctx, key)
	cancel()
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, errors.New("have not found key/value!")
	}

	return resp.Kvs[0].Value, nil
}

func (e *EtcdConn) GetWithChild(key string) ([]KeyValue, error) {

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	resp, err := e.client.Get(ctx, key, clientv3.WithPrefix())
	cancel()
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, errors.New("have not found key/value!")
	}

	var kvs []KeyValue

	for _, v := range resp.Kvs {
		kv := KeyValue{Key: string(v.Key), Value: string(v.Value)}
		kvs = append(kvs, kv)
	}

	return kvs, nil
}

func (e *EtcdConn) Watch(ctx context.Context, key string) <-chan KvWatchRsq {

	var act EVENT_TYPE
	var value string

	watchrsq := make(chan KvWatchRsq, 100)

	wch := e.client.Watch(ctx, key, clientv3.WithPrefix(), clientv3.WithPrevKV())

	go func() {

		select {
		case wrsp := <-wch:
			{
				for _, event := range wrsp.Events {

					switch event.Type {
					case mvcc.PUT:
						{
							key = string(event.Kv.Key)
							value = string(event.Kv.Value)
							if event.Kv.Version == 1 {
								act = EVENT_ADD
							} else {
								act = EVENT_UPDATE
							}
						}

					case mvcc.DELETE:
						{
							if event.PrevKv == nil {
								log.Println("prev kv is not exist!")
								continue
							}

							act = EVENT_DELETE
							key = string(event.Kv.Key)
							value = string(event.Kv.Value)
							lease := event.PrevKv.Lease

							if lease == 0 {
								break
							}

							ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
							resp, err := e.client.TimeToLive(ctx, clientv3.LeaseID(lease))
							cancel()

							if err != nil {
								break
							}

							if resp.TTL == -1 {
								act = EVENT_EXPIRE
							}
						}
					default:
						continue
					}

					watchrsq <- KvWatchRsq{Act: act, Key: key, Value: string(value)}
				}
			}
		case <-ctx.Done():
			return
		}
	}()

	return watchrsq
}
