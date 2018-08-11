package broker

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
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

	fmt.Printf("Cluster Name  : %s\r\n", clustername)
	fmt.Printf("Public conf   : [%d,%d]\r\n", publiccfg.PartitionNum, publiccfg.ReplicasNum)
	fmt.Printf("Node Num      : %d\r\n", len(brokerlist))
	fmt.Printf("Topic Num     : %d\r\n", len(topiclist))
	fmt.Printf("Partition Num : %d\r\n", len(partitionlist))

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
		err = BrokerPublicPut(etcdconn, cfg)
		if err != nil {
			log.Fatalln(err.Error())
		} else {
			log.Println("update public configure success!", clustername, cfg)
		}
		return
	}

	flagHelp()
}
