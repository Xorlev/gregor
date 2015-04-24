package main

import (
	"fmt"
	"github.com/xorlev/gregor/storage"
)

func main() {
	messageSet, err := storage.Open("/tmp/00000000000000000000.log")
	if err != nil {

	}
	defer messageSet.Close()

	messageSet.Append(0, &storage.Message{Key: []byte{}, Value: []byte("hello world!")})
	messageSet.Append(1, &storage.Message{Key: []byte("k2"), Value: []byte("indeed2")})
	messageSet.Append(2, &storage.Message{Key: []byte("k2"), Value: []byte("indeed3")})
	fmt.Println(messageSet.Read(0, 3))
}
