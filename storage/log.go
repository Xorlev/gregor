package storage

import ()

/* A log wraps operations around log segments, mainly important metadata
such as time the log segment was last fsynced and the current offsets / current segment.

A log should consist of multiple segments, but for simplicity's sake
this currently only implements a Log with a single LogSegment
*/

type Log interface {
	Restore()
	Roll() error
	Flush() error
	Delete() error
}
