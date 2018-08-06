package broker

type offset_t uint64

type Endpoint struct {
	Address string `json:"address"`
	Port    uint16 `json:"port"`
}

var CLUSTER_NAME string = "default"
