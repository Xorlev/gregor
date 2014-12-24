Gregor
======

*Gregor* is minimalistic Kafka clone written in Go. For educational purposes only, and does not yet strive to maintain compatibility with the Kafka protocol. 
Several sections may appear to be close transliterations of Kafka, except in Go. This is *ok*, don't panic. Breathe, and move on to another section.

###Running

```shell
$ go build
$ ./gregor
```

Right now, Gregor merely spits out a log segment file in /tmp/msgset

###TODO
* File-backed log impl
* Anything resembling safe file IO
* Log indexing
* Metadata storage
* Topic and partition implementation
* Network RPC layer
* ZK registration of brokers
