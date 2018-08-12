package broker

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
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

type PartitionManager struct {
	sync.RWMutex
	BrokerName   string
	PartitionCfg map[string]DataPartition
	PartitionSeg map[string]*Partition
	watchctx     context.Context
	cancel       context.CancelFunc
}

var gPartitionMng PartitionManager

func init() {
	gPartitionMng.PartitionCfg = make(map[string]DataPartition, 0)
	gPartitionMng.PartitionSeg = make(map[string]*Partition, 0)
	gPartitionMng.watchctx, gPartitionMng.cancel = context.WithCancel(context.Background())
}

func (p *PartitionManager) Add(partition DataPartition) {
	p.Lock()
	defer p.Unlock()

	_, b := p.PartitionCfg[partition.PartitionID]
	if b == false {
		log.Println("partition configure add: ", partition)
		p.PartitionCfg[partition.PartitionID] = partition
	} else {
		log.Println("partition configure update: ", partition)
		p.PartitionCfg[partition.PartitionID] = partition
	}

	for _, one := range p.PartitionCfg {

		_, exist := p.PartitionSeg[one.PartitionID]
		if exist {
			continue
		}

		for _, rep := range one.Replicas {
			if rep.Broker == p.BrokerName {
				partitionSeg := NewPartition(one.PartitionID, rep.Role)
				if partitionSeg != nil {
					log.Println("add partition segment success!", partitionSeg)
					p.PartitionSeg[one.PartitionID] = partitionSeg
				}
			}
		}
	}
}

func (p *PartitionManager) Put(partitionId string, message []byte) (uint64, error) {
	p.Lock()
	defer p.Unlock()

	partseg, exist := p.PartitionSeg[partitionId]
	if exist == false {
		log.Println("partition is not exist!", partitionId)
		return INVALID_OFFSET, errors.New("partition is not exist!")
	}

	return partseg.Write(message), nil
}

func (p *PartitionManager) Get(partitionId string, offset uint64) ([]byte, error) {
	p.Lock()
	defer p.Unlock()

	partseg, exist := p.PartitionSeg[partitionId]
	if exist == false {
		log.Println("partition is not exist!", partitionId)
		return nil, errors.New("partition is not exist!")
	}

	return partseg.Read(offset), nil
}

func BrokerPartitionInit(etcdconn *EtcdConn) {

	partitionChan := BrokerPartitionWatch(gPartitionMng.watchctx, etcdconn)

	go func() {
		for {
			partition := <-partitionChan
			gPartitionMng.Add(partition)
		}
	}()

	partitionlist := BrokerPartitionGet(etcdconn)
	if len(partitionlist) == 0 {
		return
	}

	for _, v := range partitionlist {
		gPartitionMng.Add(v)
	}
}

func BrokerStart(name string, endpoint string, etcds []string) error {
	etcdconn, err := NewEtcdClient(etcds)
	if err != nil {
		return err
	}
	gEtcd = etcdconn

	gPartitionMng.BrokerName = name

	err = BrokerRegister(name, endpoint)
	if err != nil {
		return err
	}

	BrokerPartitionInit(etcdconn)

	for {
		time.Sleep(1 * time.Second)
	}
}
