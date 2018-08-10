package broker

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

type PART_S int

const (
	_ PART_S = iota
	PART_S_FREE
	PART_S_PRIMARY
	PART_S_FOLLOW
)

type Partition struct {
	sync.RWMutex

	ID     string `json:"id"`
	Topic  string `json:"topic"`
	Status PART_S `json:"status"`

	DirPath string
	Offset  uint64

	seglist *SegList
}

func MkDir(file string) error {
	fileinfo, err := os.Stat(file)
	if err != nil {
		if os.IsNotExist(err) {
			err := os.Mkdir(file, os.ModePerm)
			if err != nil {
				return err
			}
			return nil
		}
		return err
	}
	if !fileinfo.IsDir() {
		return errors.New(file + "is not file dir!")
	}
	return nil
}

func WorkPath(id string) string {

	err := MkDir(CLUSTER_NAME)
	if err != nil {
		log.Fatalln(err.Error())
	}
	path := fmt.Sprintf("./%s/%s", CLUSTER_NAME, id)
	err = MkDir(path)
	if err != nil {
		log.Fatalln(err.Error())
	}

	return path
}

func CovPath(path string) []*Segment {

	dir, err := os.Open(path)
	if err != nil {
		return nil
	}

	fileinfo, err := dir.Readdir(0)
	if err != nil {
		return nil
	}

	seglist := make([]*Segment, 0)

	for _, v := range fileinfo {

		filename := v.Name()

		if v.IsDir() || -1 == strings.Index(filename, ".idx") {
			continue
		}

		idx := strings.Index(filename, ".idx")

		offset, err := strconv.Atoi(filename[:idx])
		if err != nil {
			log.Println(err.Error())
			continue
		}

		seg := NewSegment(path, uint64(offset))
		if seg == nil {
			continue
		}

		seglist = append(seglist, seg)
	}

	return seglist
}

func NewPartition(id string, status PART_S) *Partition {

	part := new(Partition)
	part.ID = id
	part.Status = status
	part.DirPath = WorkPath(id)
	part.seglist = NewSegList()

	addseglist := CovPath(part.DirPath)
	if len(addseglist) > 0 {
		part.seglist.Add(addseglist...)
	} else {
		seg := NewSegment(part.DirPath, 0)
		part.seglist.Add(seg)
	}
	part.Offset = part.seglist.Last().End()

	return part
}

func (part *Partition) CurOffset() uint64 {
	return part.Offset
}

func (part *Partition) Write(message []byte) uint64 {

	part.Lock()
	defer part.Unlock()

	part.Offset++

	for {
		err := part.seglist.Last().Write(part.Offset, message)
		if err == nil {
			break
		}
		if err == ErrIsFull {
			seg := NewSegment(part.DirPath, part.Offset)
			part.seglist.Add(seg)
		} else {
			log.Fatalln(err.Error())
		}
	}

	return part.Offset
}

func (part *Partition) Read(id uint64) []byte {

	part.RLock()
	defer part.RUnlock()

	seg := part.seglist.Find(id)
	if seg == nil {
		return nil
	}

	return seg.Read(id)
}

func (part *Partition) UpdateStatus(status PART_S) {
	part.Lock()
	defer part.Unlock()

	part.Status = status
}

func (part *Partition) Reset() {
	part.Lock()
	defer part.Unlock()

	// 重置所有内容
	if part.Offset != 0 {
		part.Offset = 0
		part.seglist.Destory()
		seg := NewSegment(part.DirPath, 0)
		part.seglist.Add(seg)
	}
}
