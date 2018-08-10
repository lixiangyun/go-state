package broker

//	"fmt"
//	"log"

//	"testing"

/*
func TestPartition01(t *testing.T) {
	part := NewPartition("0x123456789")
	if part == nil {
		t.Errorf("new partition failed!")
		return
	}

	for i, v := range part.seglist.array {
		log.Println(i, v.Begin(), v.End())
	}

	log.Println("offset : ", part.CurOffset())

	for i := 0; i < 10000; i++ {
		part.Write([]byte(fmt.Sprintf("helloworld%d_%s", i, UUID(UUID32))))
	}

	log.Println("offset : ", part.CurOffset())
}*/
