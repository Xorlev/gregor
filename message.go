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
	CrcOffset         = 0
	CrcLength         = 4
	MagicOffset       = CrcOffset + CrcLength // 4
	MagicLength       = 1
	AttributesOffset  = MagicOffset + MagicLength // 5
	AttributesLength  = 1
	KeySizeOffset     = AttributesOffset + AttributesLength // 6
	KeySizeLength     = 4
	KeyOffset         = KeySizeOffset + KeySizeLength
	PayloadSizeLength = 4

	MessageHeaderSize = CrcLength + MagicLength + AttributesLength + KeySizeLength
)

type Message struct {
	Offset     uint64
	Checksum   uint32
	Version    byte
	Attributes byte
	Key        []byte
	Payload    []byte
}

func (m *Message) String() string {
	return fmt.Sprintf("Message[offset=%d, payload=%s]", m.Offset, string(m.Payload))
}

func (msg *Message) size() uint32 {
	return uint32(8 + len(msg.Payload))
}

func (msg *Message) WriteBuffer() []byte {
	bufSize := uint32(MessageHeaderSize + len(msg.Key) + PayloadSizeLength + len(msg.Payload))
	keySize := uint32(len(msg.Key))
	payloadSize := uint32(len(msg.Payload))

	buffer := make([]byte, bufSize)
	encodeBuf := make([]byte, 8)

	var offset = uint32(0)

	// Magic (5)
	// Attributes (6)

	// Key -- is -1 if 0-length
	offset = KeySizeOffset
	if keySize == 0 {
		copy(encodeBuf, unsafeCastInt32ToBytes(-1))
	} else {
		binary.BigEndian.PutUint32(encodeBuf, keySize)
	}
	copy(buffer[offset:offset+KeySizeLength], encodeBuf)

	// Key
	offset = KeyOffset
	if keySize > 0 {
		copy(buffer[offset:offset+keySize], msg.Key)
	}

	// Payload Size
	binary.BigEndian.PutUint32(encodeBuf, payloadSize)
	offset += keySize
	copy(buffer[offset:offset+PayloadSizeLength], encodeBuf)

	// Payload
	offset += PayloadSizeLength
	fmt.Printf("Writing payload: [%d:%d]", offset, offset+payloadSize)
	copy(buffer[offset:offset+payloadSize], msg.Payload)

	// Take CRC of all bytes after the CRC
	crc := crc32.ChecksumIEEE(buffer[MagicOffset:])
	binary.BigEndian.PutUint32(encodeBuf, crc)
	copy(buffer[CrcOffset:CrcOffset+CrcLength], encodeBuf)

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

	payloadLengthOffset := KeyOffset + uint32(keyLen)
	payloadLen := binary.BigEndian.Uint32(messageSlice[payloadLengthOffset : payloadLengthOffset+PayloadSizeLength])

	payloadOffset := payloadLengthOffset + PayloadSizeLength

	msg := &Message{
		Offset:     offset,
		Checksum:   binary.BigEndian.Uint32(messageSlice[CrcOffset : CrcOffset+CrcLength]),
		Version:    messageSlice[MagicOffset : MagicOffset+MagicLength][0],
		Attributes: messageSlice[AttributesOffset : AttributesOffset+AttributesLength][0],
		Key:        keySlice,
		Payload:    messageSlice[payloadOffset : payloadOffset+payloadLen],
	}

	payloadCrc := msg.Checksum
	expectedCrc := crc32.ChecksumIEEE(messageSlice[MagicOffset : payloadOffset+payloadLen])
	if expectedCrc != payloadCrc {
		return nil, fmt.Errorf("Expected crc32 for message payload didn't match: %d != %d",
			expectedCrc, payloadCrc)
	}

	return msg, nil
}

func unsafeCastInt32ToBytes(val int32) []byte {
	hdr := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&val)), Len: 4, Cap: 4}
	return *(*[]byte)(unsafe.Pointer(&hdr))
}
