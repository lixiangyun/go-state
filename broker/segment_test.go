package broker

import (
	"bytes"
	"log"
	"os"
	"testing"
)

func TestSegment01(t *testing.T) {

	seg, err := NewSegment("segment_test_01.dat")
	if err != nil {
		t.Error(err.Error())
		return
	}

	log.Println("seg : ", seg.Begin, seg.End, seg.FileSize)

	for i := 0; i < 1000; i++ {
		seg.Write(offset_t(i), []byte("helloworld!"))
	}

	seg.Close()
}

func TestSegment02(t *testing.T) {

	seg, err := NewSegment("segment_test_01.dat")
	if err != nil {
		t.Error(err.Error())
		return
	}

	log.Println("seg : ", seg.Begin, seg.End, seg.FileSize)

	for i := 1000; i < 2000; i++ {
		seg.Write(offset_t(i), []byte("helloworld!"))
	}

	seg.Close()
}

func TestSegment03(t *testing.T) {

	seg, err := NewSegment("segment_test_01.dat")
	if err != nil {
		t.Error(err.Error())
		return
	}

	log.Println("seg : ", seg.Begin, seg.End, seg.FileSize)

	for i := 0; i < 2000; i++ {
		message, err := seg.Read(offset_t(i))
		if err != nil {
			t.Error(err.Error())
			break
		}

		if 0 != bytes.Compare(message, []byte("helloworld!")) {
			t.Error(message)
		}
	}

	seg.Close()

	err = os.Remove("segment_test_01.dat")
	if err != nil {
		t.Error(err.Error())
	}
}

func TestSegment04(t *testing.T) {

	filename := "segment_test_02.dat"

	seg, err := NewSegment(filename)
	if err != nil {
		t.Error(err.Error())
		return
	}

	log.Println("seg : ", seg.Begin, seg.End, seg.FileSize)

	for i := 0; ; i++ {
		err := seg.Write(offset_t(i), []byte("abcdefasdlinfhlkxcnlvks;df09&*(*@)$*HCLJB"))
		if err != nil {
			if err != ErrIsFull {
				t.Error(err.Error())
			}
			break
		}
	}

	log.Println("seg : ", seg.Begin, seg.End, seg.FileSize)

	seg.Close()

	fileinfo, _ := os.Stat(filename)

	log.Println("size : ", fileinfo.Size())

	os.Remove(filename)
}
