package broker

import (
	"os"
)

type Record struct {
	Offset  int
	Topic   string
	Message string
}

type LogManager struct {
	Partition string
	FileNum   int
	MaxSize   int
	Offset    int
	File      *os.File
}

var (
	DEFAULT_DIR = "./data"
)

func init() {

}
