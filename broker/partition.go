package broker

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
)

type P_STATUS string

var (
	P_FREE P_STATUS = "free"
	P_USED P_STATUS = "used"
)

type Partition struct {
	ID     string   `json:"id"`
	Topic  string   `json:"topic"`
	Status P_STATUS `json:"status"`

	DirPath string

	Offset offset_t
	SegIdx int
	SegNum int
	SegMap map[int]*Segment
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

func NewPartition(id string) (*Partition, error) {
	err := MkDir(CLUSTER_NAME)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}
	path := fmt.Sprintf("./%s/%s", CLUSTER_NAME, id)
	err = MkDir(path)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	part := new(Partition)
	part.ID = id
	part.Status = P_FREE
	part.DirPath = path
	part.SegNum = 0
	part.SegMap = make(map[int]*Segment, 0)

	dir, err := os.Open(part.DirPath)
	if err != nil {
		return nil, err
	}

	fileinfo, err := dir.Readdir(0)
	if err != nil {
		return nil, err
	}

	seglist := make(SegList, 0)
	for _, v := range fileinfo {
		if v.IsDir() {
			continue
		}

		offset, err := strconv.Atoi(v.Name())
		if err != nil {
			log.Println(err.Error())
			continue
		}

		seg := NewSegment(part.DirPath, offset_t(offset))
		if seg == nil {
			continue
		}
		seglist = append(seglist, seg)
	}

	if len(seglist) > 0 {
		sort.Sort(seglist)
		for idx, v := range seglist {
			part.SegMap[idx] = v
		}
		part.SegIdx = len(seglist) - 1
		part.Offset = part.SegMap[part.SegIdx].End()
		part.SegNum = len(seglist)
	} else {
		part.NewSegment(0)
	}

	return part, nil
}

func (p *Partition) NewSegment(id offset_t) {
	seg := NewSegment(p.DirPath, id)
	if seg != nil {
		log.Fatalln("new segmant failed!")
	}
	p.SegMap[p.SegNum] = seg
	p.SegIdx = p.SegNum
	p.SegNum++
}

func (p *Partition) Write(id offset_t, message []byte) error {

	return nil
}

func (p *Partition) Read(id offset_t) []byte {

	return nil
}
