package broker

import (
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"sync"

	"encoding/binary"
)

var (
	SEGMENT_MAXSIZE = 1024 * 1024 * 64 // 当个文件最大 64MB
)

var (
	ErrIsFull = errors.New("semaget is full!")
)

type LogRecord struct {
	size    uint64 // message body length
	crc64   uint64 // message + offset sum crc
	offset  uint64 // message offset id
	message []byte // message body context
}

type Segment struct {
	sync.RWMutex

	Begin    offset_t
	End      offset_t
	Records  map[offset_t]*LogRecord
	FileSize uint64
	FileFd   *os.File
}

func (rec *LogRecord) Check() bool {
	crctab := crc64.New(crc64.MakeTable(crc64.ISO))
	crctab.Write(rec.message)
	if rec.crc64 == crctab.Sum64() {
		return true
	} else {
		return false
	}
}

func (rec *LogRecord) Crc() {
	crctab := crc64.New(crc64.MakeTable(crc64.ISO))
	crctab.Write(rec.message)
	rec.crc64 = crctab.Sum64()
}

func RecordRead(r io.Reader) ([]*LogRecord, error) {
	loglist := make([]*LogRecord, 0)
	buffer := make([]byte, 1024)
	remain := 0

	for {
		cnt, err := r.Read(buffer[remain:])
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		remain = cnt + remain
		if remain == 0 {
			break
		}

		index := 0

		for {

			if remain < 24 {
				copy(buffer[0:remain], buffer[index:index+remain])
				break
			}

			size := binary.BigEndian.Uint64(buffer[index : index+8])
			if remain < int(size+24) {
				copy(buffer[0:remain], buffer[index:index+remain])
				break
			}

			rec := new(LogRecord)
			rec.size = size
			rec.crc64 = binary.BigEndian.Uint64(buffer[index+8 : index+16])
			rec.offset = binary.BigEndian.Uint64(buffer[index+16 : index+24])
			rec.message = make([]byte, int(size))
			copy(rec.message, buffer[index+24:index+24+int(size)])

			if rec.Check() == false {
				errstr := fmt.Sprintf("bad record(%v) sum64 check failed!", rec)
				return nil, errors.New(errstr)
			}

			loglist = append(loglist, rec)

			remain -= int(size + 24)
			index += int(size + 24)
		}
	}

	return loglist, nil
}

func RecordWrite(rec *LogRecord, w io.Writer) error {
	buffer := make([]byte, 24+rec.size)

	binary.BigEndian.PutUint64(buffer[:8], rec.size)
	binary.BigEndian.PutUint64(buffer[8:16], rec.crc64)
	binary.BigEndian.PutUint64(buffer[16:24], rec.offset)
	copy(buffer[24:], rec.message)

	index := 0
	for {
		cnt, err := w.Write(buffer[index:])
		if err != nil {
			return err
		}
		if cnt+index >= len(buffer) {
			break
		}
		index += cnt
	}

	return nil
}

func NewSegment(filename string) (*Segment, error) {

	seg := new(Segment)
	seg.Begin = ^offset_t(0)
	seg.End = 0
	seg.Records = make(map[offset_t]*LogRecord, 1024)

	fileinfo, err := os.Stat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			seg.FileSize = 0
			fd, err := os.Create(filename)
			if err != nil {
				return nil, err
			}
			seg.FileFd = fd
			return seg, nil
		}
	}

	seg.FileFd, err = os.OpenFile(filename, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}

	seg.FileSize = uint64(fileinfo.Size())

	loglist, err := RecordRead(seg.FileFd)
	if err != nil {
		return nil, err
	}

	for _, v := range loglist {
		offset_id := offset_t(v.offset)
		seg.Records[offset_id] = v
		if seg.Begin > offset_id {
			seg.Begin = offset_id
		}
		if seg.End < offset_id {
			seg.End = offset_id
		}
	}

	seg.FileFd.Seek(int64(seg.FileSize), 0)

	return seg, nil
}

func (s *Segment) Write(id offset_t, msg []byte) error {
	s.Lock()
	defer s.Unlock()

	if s.FileSize >= uint64(SEGMENT_MAXSIZE) {
		return ErrIsFull
	}

	rec := new(LogRecord)
	rec.size = uint64(len(msg))
	rec.offset = uint64(id)
	rec.message = make([]byte, len(msg))
	copy(rec.message, msg)
	rec.Crc()

	err := RecordWrite(rec, s.FileFd)
	if err == nil {
		if s.Begin > id {
			s.Begin = id
		}
		if s.End < id {
			s.End = id
		}
		s.Records[offset_t(rec.offset)] = rec
		s.FileSize += (rec.size + 24)
	}

	return err
}

func (s *Segment) Read(id offset_t) ([]byte, error) {
	s.RLock()
	defer s.RUnlock()

	rec, b := s.Records[id]
	if b == false {
		return nil, errors.New(fmt.Sprintf("offset %v not exist!", id))
	}

	return rec.message, nil
}

func (s *Segment) Close() {

	s.Lock()
	defer s.Unlock()

	s.FileFd.Close()
	s.Records = nil
}
