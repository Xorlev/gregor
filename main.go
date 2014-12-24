package main

import (
	"fmt"
	"os"
)

func main() {
	messageSet := new(FileMessageSet)
	messageSet.f, _ = os.OpenFile("/tmp/msgset", os.O_CREATE|os.O_RDWR, 0666)

	messageSet.Append(&Message{Offset: 1122334455, Payload: []byte("hello world!")})
	messageSet.Append(&Message{Offset: 9999999999, Payload: []byte("indeed")})
	fmt.Println(messageSet.Read(0, 2))
}
