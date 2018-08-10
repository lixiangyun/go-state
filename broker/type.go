package broker

const CLUSTER_NAME = "default"

const KEY_COMMON = "/" + CLUSTER_NAME + "/common"

type DataCommon struct {
	PartitionNum int `json:"partitions"`
	ReplicasNum  int `json:"replicas"`
}

type PartReplicas struct {
	Broker string `json:"broker"`
	Role   PART_S `json:"status"`
}

const KEY_PARTITION = "/" + CLUSTER_NAME + "/partition/"

type DataPartition struct {
	PartitionID string         `json:"partitionid"`
	Topic       string         `json:"topic"`
	Replicas    []PartReplicas `json:"replicas"`
}

const KEY_BROKER = "/" + CLUSTER_NAME + "/broker/"

type DataBroker struct {
	Broker string `json:"broker"`
	Addr   string `json:"endpoint"`
}

const KEY_TOPIC = "/" + CLUSTER_NAME + "/topic/"

type DataTopic struct {
	Topic       string `json:"topic"`
	PartitionID string `json:"partitionid"`
}

type DataSubscribe struct {
	Topic  string `json:"topic"`
	Offset uint64 `json:"offset"`
}

const KEY_CONSUMER = "/" + CLUSTER_NAME + "/consumer/"

type DataConsumer struct {
	ConsumerID string          `json:"consumerid"`
	Subs       []DataSubscribe `json:"subs"`
}

const (
	INVALID_OFFSET = ^uint64(0)
)
