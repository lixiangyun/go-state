package broker

const (
	INVALID_OFFSET = ^uint64(0)
)

type Endpoint struct {
	Address string `json:"address"`
	Port    uint16 `json:"port"`
}

var CLUSTER_NAME string = "default"
