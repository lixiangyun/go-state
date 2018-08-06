package broker

type BrokerState struct {
	Name string   `json:"name"`
	Addr Endpoint `json:"endpoint"`
}
