package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"reflect"
	"unsafe"
)

/**
 * A message. The format of an N byte message is the following:
 *
 * 1. 4 byte CRC32 of the message (computed after the rest written into the buffer, IEEE)
 * 2. 1 byte "magic" identifier to allow format changes, value is 2 currently
 * 3. 1 byte "attributes" identifier to allow annotations on the message independent of the version (e.g. compression enabled, type of codec used)
 * 4. 4 byte key length, containing length K (-1 if none)
 * 5. K byte key
 * 6. 4 byte payload length, containing length V
 * 7. V byte payload
 *
 * Default constructor wraps an existing ByteBuffer with the Message object with no change to the contents.

Kafka message on-disk:
0-4 CRC32
4 Magic
5 Attributes (inc. compression info)
6-10 Key length
10-n Key
n-n+4 Payload Length
n+4-m Payload


*/
const (
	CrcOffset        = 0
	CrcLength        = 4
	MagicOffset      = CrcOffset + CrcLength // 4
	MagicLength      = 1
	AttributesOffset = MagicOffset + MagicLength // 5
	AttributesLength = 1
	KeySizeOffset    = AttributesOffset + AttributesLength // 6
	KeySizeLength    = 4
	KeyOffset        = KeySizeOffset + KeySizeLength
	ValueSizeLength  = 4

	MessageHeaderSize = CrcLength + MagicLength + AttributesLength + KeySizeLength
)

type Message struct {
	Offset     uint64
	Checksum   uint32
	Version    byte
	Attributes byte
	Key        []byte
	Value      []byte
}

func (msg *Message) String() string {
	return fmt.Sprintf("Message[offset=%d, key=%s, value=%s]", msg.Offset, string(msg.Key), string(msg.Value))
}

func (msg *Message) size() uint32 {
	return uint32(MessageHeaderSize + len(msg.Key) + ValueSizeLength + len(msg.Value))
}

func (msg *Message) keySize() uint32 {
	return uint32(len(msg.Key))
}

func (msg *Message) valueSize() uint32 {
	return uint32(len(msg.Value))
}

func (msg *Message) WriteBuffer() []byte {
	keySize := msg.keySize()
	valueSize := msg.valueSize()

	buffer := make([]byte, msg.size())

	var offset = uint32(0)
	offset += CrcLength
	// Magic (5)
	offset += MagicLength
	// Attributes (6)
	offset += AttributesLength

	// Key -- is -1 if 0-length
	if keySize == 0 {
		copy(buffer[offset:], unsafeCastInt32ToBytes(-1))
	} else {
		binary.BigEndian.PutUint32(buffer[offset:], keySize)
	}
	offset += KeySizeLength

	// Key
	if keySize > 0 {
		copy(buffer[offset:], msg.Key)
	}
	offset += keySize

	// Payload Size
	binary.BigEndian.PutUint32(buffer[offset:], valueSize)
	offset += ValueSizeLength

	// Payload
	copy(buffer[offset:], msg.Value)
	offset += uint32(len(msg.Value))

	// Take CRC of all bytes after the CRC
	crc := crc32.ChecksumIEEE(buffer[MagicOffset:])
	binary.BigEndian.PutUint32(buffer[CrcOffset:], crc)

	return buffer
}

func hydrateMessage(messageSlice []byte, offset uint64) (*Message, error) {
	var keyLen = int32(binary.BigEndian.Uint32(messageSlice[KeySizeOffset : KeySizeOffset+KeySizeLength]))

	// If keylen == -1, then we skip it
	var keySlice []byte
	if keyLen == -1 {
		keyLen = 0
		keySlice = []byte{}
	} else {
		keySlice = messageSlice[KeyOffset : KeyOffset+keyLen]
	}

	valueLengthOffset := KeyOffset + uint32(keyLen)
	valueLen := binary.BigEndian.Uint32(messageSlice[valueLengthOffset : valueLengthOffset+ValueSizeLength])

	valueOffset := valueLengthOffset + ValueSizeLength

	msg := &Message{
		Offset:     offset,
		Checksum:   binary.BigEndian.Uint32(messageSlice[CrcOffset : CrcOffset+CrcLength]),
		Version:    messageSlice[MagicOffset : MagicOffset+MagicLength][0],
		Attributes: messageSlice[AttributesOffset : AttributesOffset+AttributesLength][0],
		Key:        keySlice,
		Value:      messageSlice[valueOffset : valueOffset+valueLen],
	}

	expectedCrc := crc32.ChecksumIEEE(messageSlice[MagicOffset : valueOffset+valueLen])
	if expectedCrc != msg.Checksum {
		return nil, fmt.Errorf("Expected crc32 for message payload didn't match: %d != %d",
			expectedCrc, msg.Checksum)
	}

	return msg, nil
}

func unsafeCastInt32ToBytes(val int32) []byte {
	hdr := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&val)), Len: 4, Cap: 4}
	return *(*[]byte)(unsafe.Pointer(&hdr))
}
