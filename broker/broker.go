package broker

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/coreos/etcd/clientv3"
)

var gEtcd *EtcdConn

func BrokerCall() *clientv3.Client {
	return gEtcd.Call()
}

func keepalive(ttl int, lease clientv3.LeaseID) {
	var trycnt int

	for {
		<-time.After(time.Duration(ttl) * time.Second / defaultTimes)

		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		_, err := BrokerCall().KeepAliveOnce(ctx, lease)
		cancel()

		if err != nil {
			trycnt++
			if trycnt > defaultTryTimes {
				log.Fatalln("broker heartbeat fail!")
			}
			continue
		}
		trycnt = 0
	}
}

func BrokerRegister(name string, endpoint string) error {

	key := KEY_BROKER + name
	brk := &DataBroker{Broker: name, Addr: endpoint}

	value, err := json.Marshal(brk)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	rasp, err := BrokerCall().Grant(ctx, int64(defaultTTL))
	cancel()
	if err != nil {
		return err
	}

	cmp := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	put := clientv3.OpPut(key, string(value), clientv3.WithLease(rasp.ID))

	ctx, cancel = context.WithTimeout(context.Background(), defaultTimeout)
	resp, err := BrokerCall().Txn(ctx).If(cmp).Then(put).Commit()
	cancel()
	if err != nil {
		return err
	}

	if resp.Succeeded {
		go keepalive(defaultTTL, rasp.ID)
		log.Println("broker [" + name + "] register success!")
		return nil
	}

	return errors.New("register [" + name + "] failed!")
}

func BrokerStart(name string, endpoint string, etcds []string) error {
	etcdconn, err := NewEtcdClient(etcds)
	if err != nil {
		return err
	}
	gEtcd = etcdconn

	err = BrokerRegister(name, endpoint)
	if err != nil {
		return err
	}

	for {
		time.Sleep(1 * time.Second)
	}
}
