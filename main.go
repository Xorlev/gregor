package main

import (
	"fmt"
)

func main() {
	messageSet := new(FileMessageSet)
	if err := messageSet.Open("/tmp/kafka-logs/hello-0/00000000000000000000.log"); err != nil {

	}
	defer messageSet.Close()

	messageSet.Append(&Message{Offset: 0, Key: []byte("k1"), Value: []byte("hello world!")})
	messageSet.Append(&Message{Offset: 1, Key: []byte("k2"), Value: []byte("indeed2")})
	fmt.Println(messageSet.Read(0, 2))
}
