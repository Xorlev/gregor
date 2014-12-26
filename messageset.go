package main

import (
	"encoding/binary"
	"fmt"
	"os"
)

// Currently hard-coded limit on message size.
const (
	MESSAGE_SIZE_LIMIT     = 4 * 1024 * 1024
	MESSAGE_FORMAT_VERSION = uint16(1)
	OffsetLength           = 8
	MessageSizeLength      = 4
	MessageOverhead        = OffsetLength + MessageSizeLength
)

type MessageSet interface {
	Read(start uint64, messages uint) ([]*Message, error)
	Append(offset uint64, message *Message) (uint64, error)
	Sync() error
	Close() error
	Delete() error
}

type FileMessageSet struct {
	MessageSet
	f *os.File
}

func Open(path string) (*FileMessageSet, error) {

	var err error
	fms := new(FileMessageSet)
	fms.f, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	return fms, nil
}

func (fms *FileMessageSet) Read(start uint64, maxMessages uint) ([]*MessageAndOffset, error) {
	var fileOffset = int64(start)

	fileStat, err := fms.f.Stat()
	if err != nil {
		return nil, err
	}

	messages := make([]*MessageAndOffset, 0)
	var messageCount = uint(0)
	for (fileOffset+MessageOverhead) < fileStat.Size() && messageCount < maxMessages {
		messageOffset, err := fms.readUint64(fileOffset)
		if err != nil {
			return nil, err
		}

		messageSize, err := fms.readUint32(fileOffset + OffsetLength)
		if err != nil {
			return nil, err
		}

		if messageSize > MESSAGE_SIZE_LIMIT {
			return nil, fmt.Errorf("Message larger than message size limit: %d > %d",
				messageSize, MESSAGE_SIZE_LIMIT)
		}

		messageBuf := make([]byte, messageSize)              // 4mb limit
		fms.f.ReadAt(messageBuf, fileOffset+MessageOverhead) // at offset + msgoffset + msgsize

		newMessage, err := hydrateMessage(messageBuf)
		if err != nil {
			return nil, err
		}

		messages = append(messages, &MessageAndOffset{messageOffset, newMessage})

		fileOffset += MessageOverhead + int64(messageSize)
		messageCount += 1
	}

	return messages, nil
}

func (fms *FileMessageSet) Append(offset uint64, message *Message) (uint64, error) {
	msgBuffer := message.WriteBuffer()

	// Seek nowhere to get our current position
	// msgStart, err := fms.f.Seek(1, 0)
	// if err != nil {
	// 	return 0, err
	// }

	// Write offset, messagelength, and message
	fms.writeData(offset)
	fms.writeData(uint32(len(msgBuffer)))
	fms.f.Write(msgBuffer)

	return uint64(0), nil
}

func (fms *FileMessageSet) Sync() error {
	return fms.f.Sync()
}

func (fms *FileMessageSet) Close() error {
	if err := fms.Sync(); err != nil {
		return err
	}

	return fms.f.Close()
}

func (fms *FileMessageSet) Delete() error {
	return os.Remove(fms.f.Name())
}

func (fms *FileMessageSet) readUint64(offset int64) (uint64, error) {
	buffer := make([]byte, 8)
	if _, err := fms.f.ReadAt(buffer, offset); err != nil {
		return 0, err
	}
	return uint64(binary.BigEndian.Uint64(buffer)), nil
}

func (fms *FileMessageSet) readUint32(offset int64) (uint32, error) {
	buffer := make([]byte, 4)
	if _, err := fms.f.ReadAt(buffer, offset); err != nil {
		return 0, err
	}
	return uint32(binary.BigEndian.Uint32(buffer)), nil
}

func (fms *FileMessageSet) readUint16(offset int64) (uint16, error) {
	buffer := make([]byte, 2)
	if _, err := fms.f.ReadAt(buffer, offset); err != nil {
		return 0, err
	}
	return uint16(binary.BigEndian.Uint16(buffer)), nil
}

func (fms *FileMessageSet) writeData(data interface{}) error {
	// TODO: more efficient writes, but this is convenient
	return binary.Write(fms.f, binary.BigEndian, data)
}
