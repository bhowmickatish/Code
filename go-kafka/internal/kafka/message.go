package kafka

type Record struct {
	Key       []byte
	Value     []byte
	Partition int
	Offset    int64
}

type Header struct {
	Key   string
	Value []byte
}
