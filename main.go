package main

import (
	"fmt"
)

func main() {
	messageSet, err := Open("/tmp/kafka-logs/hello-0/00000000000000000000.log")
	if err != nil {

	}
	defer messageSet.Close()

	messageSet.Append(0, &Message{Key: []byte{}, Value: []byte("hello world!")})
	messageSet.Append(1, &Message{Key: []byte("k2"), Value: []byte("indeed2")})
	messageSet.Append(2, &Message{Key: []byte("k2"), Value: []byte("indeed3")})
	fmt.Println(messageSet.Read(0, 3))
}
