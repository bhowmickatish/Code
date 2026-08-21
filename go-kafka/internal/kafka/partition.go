package kafka

// PartitionForKey returns the partition a keyed message is routed to,
// using the same murmur2 hash as kafka-go's Murmur2Balancer (Java/librdkafka default).
func PartitionForKey(key []byte, partitionCount int) int {
	if partitionCount <= 0 {
		return 0
	}
	if len(key) == 0 {
		return 0
	}
	return int((murmur2(key) & 0x7fffffff) % uint32(partitionCount))
}

func murmur2(data []byte) uint32 {
	const (
		seed uint32 = 0x9747b28c
		m    uint32 = 0x5bd1e995
		r    uint32 = 24
	)

	length := len(data)
	h := seed ^ uint32(length)
	for length >= 4 {
		k := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
		data = data[4:]
		length -= 4
	}

	switch length {
	case 3:
		h ^= uint32(data[2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[0])
		h *= m
	}

	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return h
}
