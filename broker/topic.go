package broker

type Topic struct {
	Name        string `json:"name"`
	PartitionID string `json:"partitionid"`
}
