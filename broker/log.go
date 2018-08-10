package broker

import (
	"os"
	"sync"
)

type LogManager struct {
	sync.RWMutex

	seg map[uint64]Segment

	Partition string
	FileNum   uint64
	MaxSize   uint64
	Offset    uint64
	File      *os.File
}

type LogApi interface {
	Write(offset uint64, body []byte) error
	Read(offset uint64) ([]byte, error)
}

func NewLogManager(partition string) *LogManager {
	return nil
}
