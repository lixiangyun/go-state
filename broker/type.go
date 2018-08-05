package broker

type Endpoint struct {
	Address string `json:"address"`
	Port    uint16 `json:"port"`
}
