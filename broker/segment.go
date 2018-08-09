package broker

import (
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"log"
	"os"

	"encoding/binary"
)

var (
	SEGMENT_MAXSIZE  = 1024 * 1024 * 64 // 当个文件最大 64MB
	SEGMENT_SYNCSIZE = 4 * 1024
	SEGMENT_SYNCCNT  = 100
)

var (
	ErrIsFull = errors.New("semgent is full!")
)

type MsgRec struct {
	crc64  uint64
	size   uint64
	offset uint64
	body   []byte
}

type MsgIdx struct {
	index  uint64
	offset uint64
}

type MsgIdxFile struct {
	fileFd   *os.File
	maxIdx   uint64
	writecnt int
}

type MsgRecFile struct {
	fileFd    *os.File
	curSize   int64
	writeSize int64
	isFull    bool
}

type Segment struct {
	bFull bool

	idx *MsgIdxFile
	log *MsgRecFile

	start offset_t
	end   offset_t
}

func (rec *MsgRec) CrcCheck() bool {
	var buffer [16]byte
	binary.BigEndian.PutUint64(buffer[:], rec.offset)
	binary.BigEndian.PutUint64(buffer[8:], rec.size)

	crctab := crc64.New(crc64.MakeTable(crc64.ISO))
	crctab.Write(rec.body)
	crctab.Write(buffer[:])

	if rec.crc64 == crctab.Sum64() {
		return true
	} else {
		return false
	}
}

func (rec *MsgRec) CrcSum() {
	var buffer [16]byte
	binary.BigEndian.PutUint64(buffer[:], rec.offset)
	binary.BigEndian.PutUint64(buffer[8:], rec.size)

	crctab := crc64.New(crc64.MakeTable(crc64.ISO))
	crctab.Write(rec.body)
	crctab.Write(buffer[:])

	rec.crc64 = crctab.Sum64()
}

func openfile(filename string) (*os.File, error) {
	_, err := os.Stat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			fd, err := os.Create(filename)
			if err != nil {
				return nil, err
			}
			return fd, nil
		}
		return nil, err
	}
	fd, err := os.OpenFile(filename, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	return fd, nil
}

func getfilesize(fd *os.File) uint64 {
	fileinfo, err := fd.Stat()
	if err != nil {
		log.Fatalln(err.Error())
		return 0
	}
	return uint64(fileinfo.Size())
}

func NewMsgIdxFile(filename string) *MsgIdxFile {
	idx := new(MsgIdxFile)
	fd, err := openfile(filename)
	if err != nil {
		log.Println(err.Error())
		return nil
	}
	idx.fileFd = fd
	filesize := getfilesize(fd)
	idx.maxIdx = filesize / 16
	return idx
}

func (idx *MsgIdxFile) Put(offset uint64) {
	var buffer [16]byte
	binary.BigEndian.PutUint64(buffer[:8], idx.maxIdx)
	binary.BigEndian.PutUint64(buffer[8:], offset)

	_, err := idx.fileFd.Seek(int64(idx.maxIdx*16), 0)
	if err != nil {
		log.Fatal(err.Error())
	}

	cnt, err := idx.fileFd.Write(buffer[:])
	if err != nil {
		log.Fatal(err.Error())
	}

	if cnt != len(buffer) {
		log.Fatalf("write msg index(%v) failed!", idx)
	}

	idx.maxIdx++
}

func (idx *MsgIdxFile) Get(index uint64) *MsgIdx {
	if idx.maxIdx < index {
		return nil
	}
	_, err := idx.fileFd.Seek(int64(index*16), 0)
	if err != nil {
		log.Println(err.Error())
		return nil
	}
	var buffer [16]byte

	cnt, err := idx.fileFd.Read(buffer[:])
	if err != nil {
		log.Println(err.Error())
		return nil
	}
	if cnt != len(buffer) {
		log.Println("read index failed!", index)
		return nil
	}
	msgidx := new(MsgIdx)
	msgidx.index = binary.BigEndian.Uint64(buffer[:])
	msgidx.offset = binary.BigEndian.Uint64(buffer[8:])

	idx.writecnt++
	if idx.writecnt > SEGMENT_SYNCCNT {
		idx.fileFd.Sync()
		idx.writecnt = 0
	}

	return msgidx
}

func (idx *MsgIdxFile) Max() uint64 {
	return idx.maxIdx
}

func getmsgbody(rd io.Reader, size int) []byte {

	var buffer [128]byte
	var rdsize = 0

	body := make([]byte, size)
	for {
		cnt, err := rd.Read(buffer[:])
		if err != nil {
			log.Println(err.Error())
			return nil
		}
		copy(body[rdsize:], buffer[:cnt])
		rdsize += cnt
		if rdsize == size {
			break
		}
	}

	return body
}

func NewMsgRecFile(filename string) *MsgRecFile {
	rec := new(MsgRecFile)
	fd, err := openfile(filename)
	if err != nil {
		log.Println(err.Error())
		return nil
	}
	rec.fileFd = fd
	rec.curSize = int64(getfilesize(fd))
	if rec.curSize > int64(SEGMENT_MAXSIZE) {
		rec.isFull = true
	}
	return rec
}

func (rec *MsgRecFile) Full() bool {
	return rec.isFull
}

func (rec *MsgRecFile) Put(id offset_t, body []byte) uint64 {

	msg := new(MsgRec)
	msg.offset = uint64(id)
	msg.size = uint64(len(body))
	msg.body = body
	msg.CrcSum()

	_, err := rec.fileFd.Seek(rec.curSize, 0)
	if err != nil {
		log.Fatal(err.Error())
		return 0
	}

	var buffer [24]byte
	binary.BigEndian.PutUint64(buffer[:], msg.crc64)
	binary.BigEndian.PutUint64(buffer[8:], msg.size)
	binary.BigEndian.PutUint64(buffer[16:], msg.offset)

	cnt, err := rec.fileFd.Write(buffer[:])
	if err != nil {
		log.Fatal(err.Error())
		return 0
	}

	if cnt != len(buffer) {
		log.Fatal("write buffer failed!", buffer)
		return 0
	}

	cnt, err = rec.fileFd.Write(msg.body)
	if err != nil {
		log.Fatal(err.Error())
		return 0
	}

	if cnt != len(msg.body) {
		log.Fatal("write body failed!", body)
		return 0
	}

	offset := rec.curSize
	rec.curSize += int64(msg.size + 24)
	rec.writeSize += int64(msg.size + 24)

	if rec.writeSize > int64(SEGMENT_SYNCSIZE) {
		rec.fileFd.Sync()
		rec.writeSize = 0
	}

	return uint64(offset)
}

func (rec *MsgRecFile) Get(offset uint64) (id offset_t, body []byte) {

	_, err := rec.fileFd.Seek(int64(offset), 0)
	if err != nil {
		log.Println(err.Error())
		return 0, nil
	}

	var buffer [24]byte

	cnt, err := rec.fileFd.Read(buffer[:])
	if err != nil {
		log.Println(err.Error())
		return 0, nil
	}

	if cnt != len(buffer) {
		log.Println("read msg reocrd failed!", cnt, buffer)
		return 0, nil
	}

	msgrec := new(MsgRec)
	msgrec.crc64 = binary.BigEndian.Uint64(buffer[:])
	msgrec.size = binary.BigEndian.Uint64(buffer[8:])
	msgrec.offset = binary.BigEndian.Uint64(buffer[16:])
	msgrec.body = getmsgbody(rec.fileFd, int(msgrec.size))

	if msgrec.body == nil {
		return 0, nil
	}

	if false == msgrec.CrcCheck() {
		log.Println("msg record crc check failed!", msgrec)
		return 0, nil
	}

	return offset_t(msgrec.offset), msgrec.body
}

func checkvalid(seg *Segment) bool {

	maxidx := seg.idx.Max()

	for i := uint64(0); i < maxidx; i++ {
		msgidx := seg.idx.Get(i)

		if msgidx.index != uint64(i) {
			log.Println(msgidx, i)
			return false
		}

		_, msgbody := seg.log.Get(msgidx.offset)
		if msgbody == nil {
			log.Println("gat msg rec failed!", msgidx)
			return false
		}
	}

	return true
}

func NewSegment(path string, start offset_t) *Segment {

	var err error

	seg := new(Segment)
	seg.start = start
	seg.end = start

	logfile := fmt.Sprintf("%s/%020u.log", path, start)
	idxfile := fmt.Sprintf("%s/%020u.idx", path, start)

	seg.idx = NewMsgIdxFile(idxfile)
	if seg.idx != nil {
		log.Fatalln(err.Error())
		return nil
	}

	seg.log = NewMsgRecFile(logfile)
	if err != nil {
		log.Fatalln(err.Error())
		return nil
	}

	if seg.idx.Max() > 0 {
		if false == checkvalid(seg) {
			log.Fatalln(err.Error())
			return nil
		}
		seg.end = start + offset_t(seg.idx.Max())
	}

	return seg
}

func (s *Segment) Write(id offset_t, body []byte) {

}

func (s *Segment) Read(id offset_t) []byte {

	return nil
}

func (s *Segment) Begin() offset_t {
	return s.start
}

func (s *Segment) End() offset_t {
	return s.end
}

func (s *Segment) Delete() error {

	return nil
}

type SegList []*Segment

func (list SegList) Len() int {
	return len(list)
}

func (list SegList) Less(i, j int) bool {
	if list[i].end < list[j].start {
		return true
	}
	return false
}

func (list SegList) Swap(i, j int) {
	tmp := list[i]
	list[i] = list[j]
	list[j] = tmp
}
