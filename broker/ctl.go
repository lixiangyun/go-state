package broker

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	DISPLAY_SEPARATOR = "------------------------------------------------"
)

var (
	clustername string
	commcfg     string
	infomation  bool
	etcdcluster string
	help        bool
	etcdconn    *EtcdConn
)

func flaginit() {

	flag.StringVar(&clustername, "cluster", "default", "the broker cluster name to operate.")
	flag.StringVar(&commcfg, "config", "", "partition public config for broker cluster.")
	flag.BoolVar(&infomation, "info", false, "display broker detail infomation.")
	flag.StringVar(&etcdcluster, "etcd", "127.0.0.1:2379", "etcd cluster address list. \"ip1:port,ip2:port...\".")
	flag.BoolVar(&help, "help", false, "this help.")

	flag.Parse()

	if help {
		flagHelp()
	}

	BrokerClusterNameSet(clustername)
}

func flagHelp() {
	flag.Usage()
	os.Exit(1)
}

func BrokerInfomation() {

	brokerlist := BrokerServerGet(etcdconn)
	topiclist := BrokerTopicGet(etcdconn)
	partitionlist := BrokerPartitionGet(etcdconn)
	publiccfg := BrokerPublicGet(etcdconn)

	fmt.Println(DISPLAY_SEPARATOR)

	fmt.Printf("Cluster Name  : %s\r\n", clustername)
	fmt.Printf("Public conf   : [%d,%d]\r\n", publiccfg.PartitionNum, publiccfg.ReplicasNum)
	fmt.Printf("Node Num      : %d\r\n", len(brokerlist))
	fmt.Printf("Topic Num     : %d\r\n", len(topiclist))
	fmt.Printf("Partition Num : %d\r\n", len(partitionlist))

	if len(brokerlist) > 0 {
		fmt.Println(DISPLAY_SEPARATOR)
	}

	for idx, v := range brokerlist {
		fmt.Printf("[%3d]node : %v\r\n", idx, v)
	}

	if len(topiclist) > 0 {
		fmt.Println(DISPLAY_SEPARATOR)
	}

	for idx, v := range topiclist {
		fmt.Printf("[%3d]topic : %v\r\n", idx, v)
	}

	if len(partitionlist) > 0 {
		fmt.Println(DISPLAY_SEPARATOR)
	}

	for idx, v := range partitionlist {
		fmt.Printf("[%3d]partition : %v\r\n", idx, v)
	}

	fmt.Println(DISPLAY_SEPARATOR)

	/* statistical analysis */
	if len(partitionlist) == 0 {
		return
	}

	brokerUsed := make(map[string]int, 0)
	brokerPrim := make(map[string]int, 0)
	brokerFlow := make(map[string]int, 0)

	partitionUsed := 0
	for _, v := range partitionlist {
		if v.Status != PART_S_FREE {
			partitionUsed++
		}

		for _, rep := range v.Replicas {
			brokerUsed[rep.Broker]++

			if rep.Role == PART_S_PRIMARY {
				brokerPrim[rep.Broker]++
			} else {
				brokerFlow[rep.Broker]++
			}
		}
	}

	fmt.Println(DISPLAY_SEPARATOR)
	fmt.Printf("partiton use statistics : %02.2f%%\r\n",
		float32(partitionUsed)/float32(len(partitionlist)))

	if len(brokerlist) == 0 {
		return
	}

	fmt.Println(DISPLAY_SEPARATOR)
	for _, v := range brokerlist {

		used, prim, flow := 0, 0, 0

		used, _ = brokerUsed[v.Broker]
		prim, _ = brokerPrim[v.Broker]
		flow, _ = brokerFlow[v.Broker]

		fmt.Printf("borker : %s (used: %d, primary: %d, follow: %d)\r\n",
			v.Broker, used, prim, flow)
	}
}

func ParseConfig(param string) DataCommon {

	var cfg DataCommon

	parms := strings.Split(param, ",")
	if len(parms) != 2 {
		log.Println("input config param is invalid!", parms)
		flagHelp()
	}

	num, err := strconv.Atoi(parms[0])
	if err != nil {
		log.Println(err.Error())
		log.Println("input config param is invalid!", parms)
		flagHelp()
	}

	cfg.PartitionNum = num

	num, err = strconv.Atoi(parms[1])
	if err != nil {
		log.Println(err.Error())
		log.Println("input config param is invalid!", parms)
		flagHelp()
	}

	cfg.ReplicasNum = num

	if cfg.PartitionNum < cfg.ReplicasNum || cfg.ReplicasNum < 2 {
		log.Println("input config param is invalid!", parms)
		flagHelp()
	}

	return cfg
}

func BrokerPartitionScale(etcdconn *EtcdConn) error {

	publiccfg := BrokerPublicGet(etcdconn)
	brokerlist := BrokerServerGet(etcdconn)
	partitionlist := BrokerPartitionGet(etcdconn)

	num := publiccfg.PartitionNum - len(partitionlist)

	if num <= 0 {
		return errors.New("No need to adjust.")
	}

	for i := 0; i < num; i++ {

		var partition DataPartition

		partition.PartitionID = UUID(UUID64)
		partition.Status = PART_S_FREE
		partition.Replicas = make([]PartReplicas, publiccfg.ReplicasNum)

		for j := 0; j < publiccfg.ReplicasNum; j++ {
			partition.Replicas[j].Broker = brokerlist[(i+j)%len(brokerlist)].Broker
			partition.Replicas[j].Role = PART_S_FOLLOW
		}
		partition.Replicas[0].Role = PART_S_PRIMARY

		err := BrokerPartitionPut(etcdconn, partition)
		if err != nil {
			return err
		}
	}

	return nil
}

func BrokerCtl() {

	flaginit()

	etcdaddr := strings.Split(etcdcluster, ",")
	log.Println("connect etcd cluster :", etcdaddr)

	etcd, err := NewEtcdClient(etcdaddr)
	if err != nil {
		log.Fatalln(err.Error())
	}

	etcdconn = etcd

	if infomation {
		BrokerInfomation()
		return
	}

	if commcfg != "" {

		cfg := ParseConfig(commcfg)

		oldcfg := BrokerPublicGet(etcdconn)
		if oldcfg == nil {
			log.Fatalln("get old configure failed!")
		}

		if cfg.PartitionNum < oldcfg.PartitionNum ||
			cfg.ReplicasNum < oldcfg.ReplicasNum {

			log.Fatalf("can not supply scale in [%d,%d -> %d,%d]",
				oldcfg.PartitionNum, oldcfg.ReplicasNum,
				cfg.PartitionNum, cfg.ReplicasNum)
		}

		err = BrokerPublicPut(etcdconn, cfg)
		if err != nil {
			log.Fatalln(err.Error())
		} else {
			log.Println("update public configure success!", clustername, cfg)
		}

		err = BrokerPartitionScale(etcdconn)
		if err != nil {
			log.Fatalln(err.Error())
		} else {
			log.Println("partition scale success!", clustername, cfg)
		}

		return
	}

	flagHelp()
}
