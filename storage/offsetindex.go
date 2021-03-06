package storage

// An offset index maintains offset-based indices into each log segment's underlying
// storage file
type OffsetIndex interface {
	Index(offset uint64, position uint64) error
}

type FileOffsetIndex struct {
	OffsetIndex
}

func (*FileOffsetIndex) Index(offset uint64, position uint64) error {
	return nil
}
