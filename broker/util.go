package broker

import (
	"fmt"
	"math/rand"
	"time"
)

type UUID_TYPE int

const (
	_ UUID_TYPE = iota
	UUID32
	UUID64
	UUID128
)

func init() {
	tm := time.Now().Nanosecond()
	rand.Seed(int64(tm))
}

func UUID(uuid UUID_TYPE) string {
	switch uuid {
	case UUID32:
		{
			return fmt.Sprintf("%x", rand.Uint32())
		}
	case UUID64:
		{
			return fmt.Sprintf("%x", rand.Uint64())
		}
	case UUID128:
		{
			return fmt.Sprintf("%x%x", rand.Uint64(), rand.Uint64())
		}
	}
	return ""
}

func TimeStampRFC1123() string {
	return time.Now().Format(time.RFC1123)
}

func TimeStamp() string {
	tm := time.Now()
	return fmt.Sprintf("%4d%02d%02d%02d%02d%02d%03d",
		tm.Year(), tm.Month(), tm.Day(),
		tm.Hour(), tm.Minute(), tm.Second(),
		tm.Nanosecond()/int(time.Millisecond))
}
