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
	SEGMENT_MAXSIZE  = 1024 * 1024 * 4 // 当个文件最大4MB
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
	offset uint64
}

type MsgIdxFile struct {
	fileFd   *os.File
	maxIdx   uint64
	writecnt int
	filename string
}

type MsgRecFile struct {
	fileFd    *os.File
	curSize   int64
	writeSize int64
	isFull    bool
	filename  string
}

type Segment struct {
	idx *MsgIdxFile
	log *MsgRecFile

	recnum uint64 //记录数量
	start  uint64 //起始偏移
	end    uint64 //结束偏移
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
	idx.filename = filename
	fd, err := openfile(filename)
	if err != nil {
		log.Println(err.Error())
		return nil
	}
	idx.fileFd = fd
	filesize := getfilesize(fd)
	idx.maxIdx = filesize / 8
	return idx
}

func (idx *MsgIdxFile) Put(offset uint64) {
	var buffer [8]byte
	binary.BigEndian.PutUint64(buffer[:], offset)

	_, err := idx.fileFd.Seek(int64(idx.maxIdx*8), 0)
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

	idx.writecnt++
	if idx.writecnt > SEGMENT_SYNCCNT {
		idx.fileFd.Sync()
		idx.writecnt = 0
	}

	idx.maxIdx++
}

func (idx *MsgIdxFile) Get(index uint64) uint64 {
	if idx.maxIdx < index {
		return INVALID_OFFSET
	}
	_, err := idx.fileFd.Seek(int64(index*8), 0)
	if err != nil {
		log.Println(err.Error())
		return INVALID_OFFSET
	}
	var buffer [8]byte

	cnt, err := idx.fileFd.Read(buffer[:])
	if err != nil {
		log.Println(err.Error())
		return INVALID_OFFSET
	}
	if cnt != len(buffer) {
		log.Println("read index failed!", index)
		return INVALID_OFFSET
	}

	return binary.BigEndian.Uint64(buffer[:])
}

func (idx *MsgIdxFile) Max() uint64 {
	return idx.maxIdx
}

func (idx *MsgIdxFile) Reset() {
	idx.maxIdx = 0
}

func (idx *MsgIdxFile) Del() {
	idx.fileFd.Close()
	err := os.Remove(idx.filename)
	if err != nil {
		log.Fatalln(err.Error())
	}
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
		if cnt > (size - rdsize) {
			cnt = (size - rdsize)
		}
		copy(body[rdsize:], buffer[:cnt])

		rdsize += cnt
		if rdsize >= size {
			break
		}
	}

	return body
}

func NewMsgRecFile(filename string) *MsgRecFile {
	rec := new(MsgRecFile)
	rec.filename = filename
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

func (rec *MsgRecFile) Reset() {
	rec.isFull = false
	rec.curSize = 0
	rec.writeSize = 0
}

func (rec *MsgRecFile) Put(id uint64, body []byte) uint64 {

	msg := new(MsgRec)
	msg.offset = uint64(id)
	msg.size = uint64(len(body))
	msg.body = body
	msg.CrcSum()

	_, err := rec.fileFd.Seek(rec.curSize, 0)
	if err != nil {
		log.Fatal(err.Error())
	}

	var buffer [24]byte
	binary.BigEndian.PutUint64(buffer[:], msg.crc64)
	binary.BigEndian.PutUint64(buffer[8:], msg.size)
	binary.BigEndian.PutUint64(buffer[16:], msg.offset)

	cnt, err := rec.fileFd.Write(buffer[:])
	if err != nil {
		log.Fatal(err.Error())
	}

	if cnt != len(buffer) {
		log.Fatal("write buffer failed!", buffer)
	}

	cnt, err = rec.fileFd.Write(msg.body)
	if err != nil {
		log.Fatal(err.Error())
	}

	if cnt != len(msg.body) {
		log.Fatal("write body failed!", body)
	}

	offset := rec.curSize
	rec.curSize += int64(msg.size + 24)
	rec.writeSize += int64(msg.size + 24)

	if rec.writeSize > int64(SEGMENT_SYNCSIZE) {
		rec.fileFd.Sync()
		rec.writeSize = 0
	}

	if rec.curSize >= int64(SEGMENT_MAXSIZE) {
		rec.isFull = true
	}

	return uint64(offset)
}

func (rec *MsgRecFile) Get(offset uint64) (id uint64, body []byte) {

	_, err := rec.fileFd.Seek(int64(offset), 0)
	if err != nil {
		log.Println(err.Error())
		return INVALID_OFFSET, nil
	}

	var buffer [24]byte

	cnt, err := rec.fileFd.Read(buffer[:])
	if err != nil {
		log.Println(err.Error())
		return INVALID_OFFSET, nil
	}

	if cnt != len(buffer) {
		log.Println("read msg reocrd failed!", cnt, buffer)
		return INVALID_OFFSET, nil
	}

	msgrec := new(MsgRec)
	msgrec.crc64 = binary.BigEndian.Uint64(buffer[:])
	msgrec.size = binary.BigEndian.Uint64(buffer[8:])
	msgrec.offset = binary.BigEndian.Uint64(buffer[16:])
	msgrec.body = getmsgbody(rec.fileFd, int(msgrec.size))

	if msgrec.body == nil {
		return INVALID_OFFSET, nil
	}

	if false == msgrec.CrcCheck() {
		log.Println("msg record crc check failed!", msgrec)
		return INVALID_OFFSET, nil
	}

	return msgrec.offset, msgrec.body
}

func (rec *MsgRecFile) Del() {
	rec.fileFd.Close()
	err := os.Remove(rec.filename)
	if err != nil {
		log.Fatalln(err.Error())
	}
}

func checkvalid(seg *Segment) bool {
	maxidx := seg.idx.Max()
	for i := uint64(0); i < maxidx; i++ {
		offset := seg.idx.Get(i)
		_, msgbody := seg.log.Get(offset)
		if msgbody == nil {
			log.Println("gat msg rec failed!", offset)
			return false
		}
	}
	return true
}

func NewSegment(path string, start uint64) *Segment {

	var err error

	seg := new(Segment)
	seg.start = start
	seg.end = start

	logfile := fmt.Sprintf("%s/%020d.log", path, start)
	idxfile := fmt.Sprintf("%s/%020d.idx", path, start)

	seg.idx = NewMsgIdxFile(idxfile)
	if seg.idx == nil {
		log.Fatalln("new idx failed!", idxfile)
		return nil
	}

	seg.log = NewMsgRecFile(logfile)
	if seg.log == nil {
		log.Fatalln("new log failed!", logfile)
		return nil
	}

	if seg.idx.Max() > 0 {
		if false == checkvalid(seg) {
			log.Println(err.Error())
			seg.idx.Reset()
			seg.log.Reset()
		}
		seg.end = start + seg.idx.Max() - 1
		seg.recnum = seg.idx.Max()
	}

	return seg
}

func (s *Segment) IsFull() bool {
	return s.log.Full()
}

func (s *Segment) Write(id uint64, body []byte) error {

	if id < s.end {
		strerr := fmt.Sprintf("input id invalid! %d, %d", id, s.end)
		return errors.New(strerr)
	}

	if s.log.Full() {
		return ErrIsFull
	}

	offset := s.log.Put(id, body)
	s.idx.Put(offset)

	s.end = id

	return nil
}

func (s *Segment) Read(id uint64) []byte {

	if id < s.start || id > s.end {
		return nil
	}

	idx := id - s.start

	offset := s.idx.Get(uint64(idx))
	_, body := s.log.Get(offset)

	return body
}

func (s *Segment) Begin() uint64 {
	return s.start
}

func (s *Segment) End() uint64 {
	return s.end
}

func (s *Segment) Find(id uint64) bool {
	if id >= s.start && id <= s.end {
		return true
	}
	return false
}

func (s *Segment) Delete() {
	s.idx.Del()
	s.log.Del()
	s.idx = nil
	s.log = nil
}

// for test api
func (s *Segment) Close() {
	s.idx.fileFd.Close()
	s.log.fileFd.Close()
	s.idx = nil
	s.log = nil
}

type SegList struct {
	array []*Segment
}

func NewSegList() *SegList {
	seglist := new(SegList)
	seglist.array = make([]*Segment, 0)
	return seglist
}

func sort(list *SegList) {
	copylist := make([]*Segment, 0)
	num := len(list.array)

	for i := 0; i < num; i++ {
		begin := ^uint64(0)
		minidx := num
		for idx, v := range list.array {
			if v == nil {
				continue
			}
			if v.Begin() < begin {
				begin = v.Begin()
				minidx = idx
			}
		}
		copylist = append(copylist, list.array[minidx])
		list.array[minidx] = nil
	}
	list.array = copylist
}

func (list *SegList) Add(seg ...*Segment) {
	list.array = append(list.array, seg...)
	sort(list)
}

func (list *SegList) Last() *Segment {
	return list.array[len(list.array)-1]
}

func (list *SegList) Del() {
	seg := list.array[0]
	seg.Delete()
	list.array = list.array[1:]
}

func (list *SegList) Destory() {
	for _, v := range list.array {
		v.Delete()
	}
	list.array = make([]*Segment, 0)
}

func (list *SegList) Find(id uint64) *Segment {
	for _, v := range list.array {
		if v.Find(id) {
			return v
		}
	}
	return nil
}
