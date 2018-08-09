package broker

import (
	"bytes"
	"log"
	"testing"
)

func TestSegment01(t *testing.T) {

	seg := NewSegment("./", 0)
	if seg == nil {
		t.Error("new segmant failed!")
		return
	}
	log.Println("seg : ", seg.Begin(), seg.End())

	for i := 0; i < 1000; i++ {
		seg.Write(offset_t(i), []byte("helloworld!"))
	}

	seg.Delete()
}

func TestSegment02(t *testing.T) {

	seg := NewSegment("./", 0)
	if seg == nil {
		t.Error("new segmant failed!")
		return
	}

	log.Println("seg : ", seg.Begin(), seg.End())

	for i := 1000; i < 2000; i++ {
		seg.Write(offset_t(i), []byte("helloworld!"))
	}

	seg.Delete()
}

func TestSegment03(t *testing.T) {

	seg := NewSegment("./", 0)
	if seg == nil {
		t.Error("new segmant failed!")
		return
	}

	log.Println("seg : ", seg.Begin(), seg.End())

	for i := 0; i < 2000; i++ {
		message := seg.Read(offset_t(i))
		if message == nil {
			t.Error("read message failed!")
			break
		}

		if 0 != bytes.Compare(message, []byte("helloworld!")) {
			t.Error(message)
		}
	}

	seg.Delete()
}

func TestSegment04(t *testing.T) {

	seg := NewSegment("./", 0)
	if seg == nil {
		t.Error("new segmant failed!")
		return
	}

	log.Println("seg : ", seg.Begin(), seg.End())

	for i := 0; ; i++ {
		seg.Write(offset_t(i), []byte("abcdefasdlinfhlkxcnlvks;df09&*(*@)$*HCLJB"))
	}

	log.Println("seg : ", seg.Begin(), seg.End())

	seg.Delete()
}
