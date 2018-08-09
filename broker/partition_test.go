package broker

import (
	"fmt"
	"log"

	"testing"
)

func TestPartition01(t *testing.T) {
	part, err := NewPartition("0x123456789")
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	for i := 0; i < part.SegNum; i++ {
		v := part.SegMap[i]
		log.Println(i, v.Begin, v.End)
	}

	offset := part.Offset
	log.Println("offset : ", offset)

	for i := 1; i < 1000000; i++ {
		part.Write(offset_t(i)+offset, []byte(fmt.Sprintf("helloworld%d_%s", i, UUID(UUID32))))
	}

}
