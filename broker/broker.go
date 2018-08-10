package broker

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/coreos/etcd/clientv3"
)

const (
	defaultTTL     = 5
	defaultTimeout = 3 * time.Second
)

var gEtcd *EtcdConn

func BrokerCall() *clientv3.Client {
	return gEtcd.Call()
}

func keepalive(ttl int, lease clientv3.LeaseID) {
	var trycnt int

	for {

		<-time.After(time.Duration(ttl) * time.Second / 3)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		_, err := BrokerCall().KeepAliveOnce(ctx, lease)
		cancel()

		if err != nil {
			trycnt++
			if trycnt > 3 {
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
		log.Println("broker" + name + "register success!")
		return nil
	}

	return errors.New("register failed!")
}

func BrokerStart(name string, endpoint string, etcds []string) error {
	etcdconn, err := NewEtcdClient(etcds, 5*time.Second)
	if err != nil {
		return err
	}
	gEtcd = etcdconn

	return BrokerRegister(name, endpoint)
}
