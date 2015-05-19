Gregor
======

*Gregor* is minimalistic Kafka clone written in Go. For educational purposes only, and does not yet strive to maintain total compatibility with the Kafka protocol. 
Several sections may appear to be close transliterations of Kafka, except in Go. This is *ok*, don't panic. Breathe, and move on to another section.

Storage layer can be used to manipulate Kafka log segments as it can write and read log segments binary compatible with Kafka 0.8.x.

There may also be value in a slimmed down implementation of Kafka which acts as a indexable log service on a single machine for testing/low-resource environments. Without multi-broker replication this would be a fairly unreliable system, but in DIY environments it wouldn't need to be.

###Running

```shell
$ go build
$ ./gregor
```

Right now, Gregor merely spits out a log segment file in /tmp/

###Roadmap
* CLI tools for manipulating Kafka log segments
  * View
  * Replay to cluster from disk or S3
* Anything resembling safe file IO
* Log indexing
* Metadata storage
* Topic and partition implementation
* Network RPC layer
* ZK registration of brokers
