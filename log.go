package main

import (
	"encoding/binary"
	"fmt"
	"os"
)

// Currently hard-coded limit on message size.
const MESSAGE_SIZE_LIMIT = 4 * 1024 * 1024

type MessageSet interface {
	Read(start uint64, messages uint) ([]*Message, error)
	Append(message *Message) error
	Close() error
}

type FileMessageSet struct {
	MessageSet
	f *os.File
}

func (fms *FileMessageSet) Read(start uint64, maxMessages uint) ([]*Message, error) {
	var offset = int64(start)
	var messageCount = uint(0)

	fileStat, err := fms.f.Stat()
	if err != nil {
		return nil, err
	}

	messages := make([]*Message, 0)
	for offset < fileStat.Size() && messageCount < maxMessages {
		messageSizeBuf := make([]byte, 4)
		fms.f.ReadAt(messageSizeBuf, offset)

		messageSize := int64(binary.BigEndian.Uint32(messageSizeBuf))
		if messageSize > MESSAGE_SIZE_LIMIT {
			return nil, fmt.Errorf("Message larger than message size limit: %d > %d",
				messageSize, MESSAGE_SIZE_LIMIT)
		}

		crc32bytes := make([]byte, 4)
		fms.f.ReadAt(crc32bytes, offset+4)
		crc := uint32(binary.BigEndian.Uint32(crc32bytes))

		messageBuf := make([]byte, messageSize) // 4mb limit
		fms.f.ReadAt(messageBuf, offset+8)

		newMessage, err := hydrateMessage(messageBuf, crc)
		if err != nil {
			return nil, err
		}

		messages = append(messages, newMessage)

		offset += messageSize + 4 + 4
		messageCount += 1
	}

	return messages, nil
}

func (fms *FileMessageSet) Append(message *Message) error {
	if err := fms.writeData(message.size()); err != nil {
		return err
	}

	if err := fms.writeData(message.Checksum()); err != nil {
		return err
	}

	if err := fms.writeData(message.Offset); err != nil {
		return err
	}

	if err := fms.writeData(message.Payload); err != nil {
		return err
	}

	return nil
}

func (fms *FileMessageSet) Close() error {
	return fms.f.Close()
}

func (fms *FileMessageSet) writeData(data interface{}) error {
	// TODO: more efficient writes, but this is convenient
	return binary.Write(fms.f, binary.BigEndian, data)
}

func int32bytes(num uint32) []byte {
	slice := make([]byte, 4)
	binary.BigEndian.PutUint32(slice, num)

	return slice
}
