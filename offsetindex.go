package main

// An offset index maintains offset-based indices into each log segment's underlying
// storage file
type OffsetIndex interface {
	Index(offset uint64, position uint64) error
}
