package broker

type BrokerState struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"`
	Cluster string   `json:"cluster"`
	Addr    Endpoint `json:"endpoint"`
}
