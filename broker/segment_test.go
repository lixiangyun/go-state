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
		err := seg.Write(uint64(i), []byte("helloworld!"))
		if err != nil {
			log.Println(err.Error())
			break
		}
	}

	for i := 0; i < 1000; i++ {
		body := seg.Read(uint64(i))
		if 0 != bytes.Compare(body, []byte("helloworld!")) {
			t.Error(body)
			break
		}
	}

	log.Println("seg : ", seg.Begin(), seg.End())

	seg.Close()
}

func TestSegment02(t *testing.T) {

	seg := NewSegment("./", 0)
	if seg == nil {
		t.Error("new segmant failed!")
		return
	}

	log.Println("seg : ", seg.Begin(), seg.End())

	for i := 1000; i < 2000; i++ {
		seg.Write(uint64(i), []byte("helloworld!"))
	}

	log.Println("seg : ", seg.Begin(), seg.End())

	seg.Close()
}

func TestSegment03(t *testing.T) {

	seg := NewSegment("./", 0)
	if seg == nil {
		t.Error("new segmant failed!")
		return
	}

	log.Println("seg : ", seg.Begin(), seg.End())

	for i := 0; i < 2000; i++ {
		message := seg.Read(uint64(i))
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

	body := make([]byte, 1024)

	for i := 0; i < len(body); i++ {
		body[i] = byte(i)
	}

	for i := 0; ; i++ {
		err := seg.Write(uint64(i), body)
		if err != nil {
			log.Println(err.Error())
			break
		}
	}

	log.Println("seg : ", seg.Begin(), seg.End())

	seg.Delete()
}
