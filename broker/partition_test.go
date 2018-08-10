package broker

import (
	"fmt"
	"log"

	"testing"
)

func TestPartition01(t *testing.T) {
	part := NewPartition("0x123456789", PART_S_FREE)
	if part == nil {
		t.Errorf("new partition failed!")
		return
	}

	for i, v := range part.seglist.array {
		log.Println(i, v.Begin(), v.End())
	}

	log.Println("offset : ", part.CurOffset())

	for i := 0; i < 100000; i++ {
		part.Write([]byte(fmt.Sprintf("helloworld%d_%s", i, UUID(UUID128))))
	}

	log.Println("offset : ", part.CurOffset())
}

func TestPartition02(t *testing.T) {
	part := NewPartition("0x123456789", PART_S_PRIMARY)
	if part == nil {
		t.Errorf("new partition failed!")
		return
	}

	for i, v := range part.seglist.array {
		log.Println(i, v.Begin(), v.End())
	}

	log.Println("offset : ", part.CurOffset())

	for i := 0; i < 100000; i++ {
		part.Write([]byte(fmt.Sprintf("helloworld%d_%s", i, UUID(UUID128))))
	}

	log.Println("offset : ", part.CurOffset())
}

func TestPartition03(t *testing.T) {
	part := NewPartition("0x123456789", PART_S_FOLLOW)
	if part == nil {
		t.Errorf("new partition failed!")
		return
	}

	part.Reset()

	log.Println("offset : ", part.CurOffset())

	for i := 0; i < 100000; i++ {
		part.Write([]byte(fmt.Sprintf("helloworld%d_%s", i, UUID(UUID128))))
	}

	log.Println("offset : ", part.CurOffset())

	part.Reset()

	log.Println("offset : ", part.CurOffset())
}
