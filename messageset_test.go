package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReadWriteLog(t *testing.T) {
	messageSet, _ := Open("/tmp/gregor-testlog-1")
	defer messageSet.Delete()

	messageSet.Append(0, &Message{Key: []byte{}, Value: []byte("hello world!")})
	messageSet.Append(1, &Message{Key: []byte("k1"), Value: []byte("v1")})
	messageSet.Append(2, &Message{Key: []byte("k2"), Value: []byte("v2")})
	messageSet.Append(3, &Message{Key: []byte("k3"), Value: []byte("v3")})

	msgs, err := messageSet.Read(0, 3)
	if err != nil {
		t.Error(err)
	}

	t.Log(msgs)

	assert.Equal(t, 0, msgs[0].Offset)
	assert.Equal(t, []byte{}, msgs[0].Message.Key)
	assert.Equal(t, []byte("hello world!"), msgs[0].Message.Value)

	assert.Equal(t, 1, msgs[1].Offset)
	assert.Equal(t, []byte("k1"), msgs[1].Message.Key)
	assert.Equal(t, []byte("v1"), msgs[1].Message.Value)

	assert.Equal(t, 3, len(msgs))
}

func TestCrc(t *testing.T) {
	messageSet, _ := Open("/tmp/gregor-testlog-1")
	defer messageSet.Delete()

	messageSet.Append(0, &Message{Key: []byte{}, Value: []byte("hello world!")})
	messageSet.Append(1, &Message{Key: []byte("k1"), Value: []byte("indeed2")})
	messageSet.Append(2, &Message{Key: []byte("k2"), Value: []byte("indeed3")})

	messageSet.f.WriteAt([]byte("oops"), 10)

	msgs, err := messageSet.Read(0, 3)
	t.Log(msgs)
	if err != nil {
		// Expected
	} else {
		t.Error("Failed to catch crc error from MessageSet.Read")
	}
}
