package main

import (
	"flag"
	"log"
	"strings"

	"github.com/lixiangyun/go-state/broker"
)

var (
	consumer    string
	etcdcluster string
	help        bool
)

func init() {
	flag.StringVar(&consumer, "name", "", "local consumer name. If not set, then using uuid.")
	flag.StringVar(&etcdcluster, "etcd", "127.0.0.1:2379", "etcd server cluster address list. such as \"ip1:port,ip2:port...\".")

	flag.BoolVar(&help, "help", false, "this help.")
}

func main() {
	flag.Parse()

	if help {
		flag.Usage()
		return
	}

	if consumer == "" {
		consumer = broker.UUID(broker.UUID64)
	}
	log.Println("consumer is", consumer)

	etcdaddr := strings.Split(etcdcluster, ",")
	log.Println("connect etcd cluster :", etcdaddr)

	err := broker.BrokerStart(name, endpoint, etcdaddr)
	if err != nil {
		log.Println(err.Error())
		return
	}
}
