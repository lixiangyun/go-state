package broker

var CLUSTER_NAME = "default"

var KEY_COMMON = "/" + CLUSTER_NAME + "/common"

type DataCommon struct {
	PartitionNum int `json:"partitions"`
	ReplicasNum  int `json:"replicas"`
}

type PartReplicas struct {
	Broker string `json:"broker"`
	Role   PART_S `json:"status"`
}

type PART_S int /* 分区状态类型 */

const (
	PART_S_FREE    PART_S = iota /* 分区空闲状态 */
	PART_S_PRIMARY               /* 分区主状态 */
	PART_S_FOLLOW                /* 分区从状态 */
)

var KEY_PARTITION = "/" + CLUSTER_NAME + "/partition/"

type DataPartition struct {
	PartitionID string         `json:"partitionid"`
	Status      PART_S         `json:"status"`
	Topic       string         `json:"topic"`
	Replicas    []PartReplicas `json:"replicas"`
}

var KEY_BROKER = "/" + CLUSTER_NAME + "/broker/"

type DataBroker struct {
	Broker string `json:"broker"`
	Addr   string `json:"endpoint"`
}

var KEY_TOPIC = "/" + CLUSTER_NAME + "/topic/"

type DataTopic struct {
	Topic       string `json:"topic"`
	PartitionID string `json:"partitionid"`
}

type DataSubscribe struct {
	Topic  string `json:"topic"`
	Offset uint64 `json:"offset"`
}

var KEY_CONSUMER = "/" + CLUSTER_NAME + "/consumer/"

type DataConsumer struct {
	ConsumerID string          `json:"consumerid"`
	Subs       []DataSubscribe `json:"subs"`
}

const (
	INVALID_OFFSET = ^uint64(0)
)

func BrokerClusterNameSet(name string) {
	CLUSTER_NAME = name
}
