package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

type Message struct {
	Offset  uint64
	Payload []byte
}

func (m *Message) Checksum() uint32 {
	return crc32.ChecksumIEEE(m.Payload)
}

func (m *Message) String() string {
	return fmt.Sprintf("Message[offset=%d, payload=%s]", m.Offset, string(m.Payload))
}

func (msg *Message) size() uint32 {
	return uint32(8 + len(msg.Payload))
}

func hydrateMessage(messageSlice []byte, expectedCrc uint32) (*Message, error) {
	buffer := bytes.NewBuffer(messageSlice)

	msg := &Message{
		Offset:  binary.BigEndian.Uint64(buffer.Next(8)),
		Payload: buffer.Bytes(),
	}

	payloadCrc := msg.Checksum()
	if expectedCrc != payloadCrc {
		return nil, fmt.Errorf("Expected crc32 for message payload didn't match: %d != %d",
			expectedCrc, payloadCrc)
	}

	return msg, nil
}
