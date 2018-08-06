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
	Offset    offset_t
	File      *os.File
}

type LogApi interface {
	Write(offset offset_t, body []byte) error
	Read(offset offset_t) ([]byte, error)
}

func NewLogManager(partition string) *LogManager {
	return nil
}
