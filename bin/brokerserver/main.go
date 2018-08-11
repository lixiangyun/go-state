package main

import (
	"flag"
	"log"
	"strings"

	"github.com/lixiangyun/go-state/broker"
)

var (
	name        string
	endpoint    string
	etcdcluster string
	help        bool
)

func init() {
	flag.StringVar(&name, "name", "", "local broker name for cluster. If not set, then using uuid.")
	flag.StringVar(&endpoint, "listen", "127.0.0.1:7001", "listen address for broker server.")
	flag.StringVar(&etcdcluster, "etcd", "127.0.0.1:2379", "etcd server cluster address list. such as \"ip1:port,ip2:port...\".")

	flag.BoolVar(&help, "help", false, "this help.")
}

func main() {

	flag.Parse()

	if help {
		flag.Usage()
		return
	}

	if name == "" {
		name = broker.UUID(broker.UUID64)
	}
	log.Println("broker name is", name)

	etcdaddr := strings.Split(etcdcluster, ",")
	log.Println("connect etcd cluster :", etcdaddr)

	err := broker.BrokerStart(name, endpoint, etcdaddr)
	if err != nil {
		log.Println(err.Error())
		return
	}
}
