package main

import (
	"sync/atomic"
	"time"
)

// A log segment wraps a MessageSet and pairs it with an index

// Slightly different than a MessageSet
type LogSegment interface {
	Read(startOffset uint64, maxOffset uint64, maxSize uint32) ([]*Message, error)
	Append(message *Message) error
	Sync() error
	Close() error
	Delete() error
	LastModified() *time.Time
	LastFlushTime() *time.Time
}

type IndexingLogSegment struct {
	LogSegment

	offsetIndex *FileOffsetIndex
	messageSet  *FileMessageSet

	nextOffset uint64
}

func (ls *IndexingLogSegment) Read(startOffset uint64, maxOffset uint64, maxSize uint32) ([]*Message, error) {
	return nil, nil
}

func (ls *IndexingLogSegment) Append(message *Message) error {
	newOffset := atomic.AddUint64(&ls.nextOffset, 1)

	// Append(offset uint64, message *Message) (uint64, error)
	position, err := ls.messageSet.Append(newOffset, message)
	if err != nil {
		return err
	}

	if err := ls.offsetIndex.Index(newOffset, position); err != nil {
		return err
	}

	return nil
}

func (ls *IndexingLogSegment) Close() error {
	return nil
}
func (ls *IndexingLogSegment) Delete() error {
	return nil
}
func (ls *IndexingLogSegment) LastModified() *time.Time {
	return nil
}
