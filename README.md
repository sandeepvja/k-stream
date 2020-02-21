## KStream - Kafka Streams for Golang

[![GoDoc](https://godoc.org/github.com/pickme-go/k-stream?status.svg)](https://godoc.org/github.com/pickme-go/k-stream)
![build](https://github.com/pickme-go/k-stream/workflows/build/badge.svg)
[![Coverage](https://codecov.io/gh/pickme-go/k-stream/branch/master/graph/badge.svg)](https://codecov.io/gh/pickme-go/k-stream)
[![Go Report Card](https://goreportcard.com/badge/github.com/pickme-go/k-stream)](https://goreportcard.com/report/github.com/pickme-go/k-stream)

KStream is a light-weight implementation of [kafka streams](https://kafka.apache.org/documentation/streams/).
It is heavily inspired by Kafka-Streams(Java) library. KStream
implements features like Internal Stores, Remote Stores, Local Tables, Global Tables and it guarantees Exactly Once 
Processing with its de-duplicator. It can process message as micro batches or one by one depending on the throughput 
required. KStream can handle good amount of throughput (50,000 messages pe second with micro batch enabled) in a fault 
tolerable manner with a  very minimal amount of latency (2 ~ 5 ms)

Project uses two external libraries
[sarama](https://github.com/Shopify/sarama) for consumers and producers

## Stream Components

### Stream Topology

Stream Topology is a set of processor nodes typically starts with a source node and ends with a sink node

### Stream Builder

Stream builder is responsible for building stream topologies into kafka streams with their dependencies like changelog 
topics, re-partition topics, etc...

### KStream

KStream is a regular kafka stream which takes an input topic as an upstream and process the record stream to another 
kafka topic(downstream topic)
It supports several functions like transforming, merging to another stream, joins with other streams etc.

### KTable

@TODO

### Global KTable

Global KTable also a KTable except for is each instance of the application has its own 
copy of all the partitions and it will be running on a separate thread so applications dose'nt have to worry 
about handling failures.

### Store

Store is a pluggable local key-val store which is used by KTable, Global KTable and other oparations like joins, merges
and removing duplicates.

### Store Backend

Store is a pluggable local key-val store which is used by KTable, Global KTable and other oparations like joins, merges
and removing duplicates.

### RPC layer for Store

@TODO

### Key discovery service

@TODO


