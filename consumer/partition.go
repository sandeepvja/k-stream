package consumer

type Partition interface {
	Wait() chan bool
	Records() <-chan *Record
	Partition() TopicPartition
}
