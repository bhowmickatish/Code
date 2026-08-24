package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// GroupLag returns the total unconsumed messages for a consumer group on a topic.
// lag per partition = log end offset - committed consumer offset
func GroupLag(ctx context.Context, groupID, topic string) (int64, error) {
	broker := BrokerList()[0]
	addr := kafka.TCP(broker)

	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		return 0, fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return 0, fmt.Errorf("read partitions: %w", err)
	}
	if len(partitions) == 0 {
		return 0, fmt.Errorf("topic %q not found", topic)
	}

	partitionIDs := make([]int, len(partitions))
	offsetReqs := make([]kafka.OffsetRequest, len(partitions))
	for i, p := range partitions {
		partitionIDs[i] = p.ID
		offsetReqs[i] = kafka.LastOffsetOf(p.ID)
	}

	client := &kafka.Client{Addr: addr}

	endOffsets, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Addr:   addr,
		Topics: map[string][]kafka.OffsetRequest{topic: offsetReqs},
	})
	if err != nil {
		return 0, fmt.Errorf("list offsets: %w", err)
	}

	committed, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		Addr:    addr,
		GroupID: groupID,
		Topics:  map[string][]int{topic: partitionIDs},
	})
	if err != nil {
		return 0, fmt.Errorf("offset fetch: %w", err)
	}
	if committed.Error != nil {
		// Group may not exist yet; treat as zero committed offsets.
		return sumLag(endOffsets.Topics[topic], nil), nil
	}

	committedByPartition := make(map[int]int64, len(partitionIDs))
	for _, p := range committed.Topics[topic] {
		if p.Error != nil {
			return 0, fmt.Errorf("offset fetch partition %d: %w", p.Partition, p.Error)
		}
		committedByPartition[p.Partition] = p.CommittedOffset
	}

	return sumLag(endOffsets.Topics[topic], committedByPartition), nil
}

func sumLag(endOffsets []kafka.PartitionOffsets, committed map[int]int64) int64 {
	var total int64
	for _, p := range endOffsets {
		if p.Error != nil {
			continue
		}
		committedOffset := int64(0)
		if committed != nil {
			if v, ok := committed[p.Partition]; ok && v >= 0 {
				committedOffset = v
			}
		}
		lag := p.LastOffset - committedOffset
		if lag > 0 {
			total += lag
		}
	}
	return total
}

type lagMonitor struct {
	mu       sync.RWMutex
	lag      int64
	maxLag   int64
	interval time.Duration
	groupID  string
	stop     chan struct{}
	done     chan struct{}
}

func newLagMonitor(ctx context.Context, maxLag int64, interval time.Duration, groupID string) *lagMonitor {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	if groupID == "" {
		groupID = GroupID
	}

	m := &lagMonitor{
		maxLag:   maxLag,
		interval: interval,
		groupID:  groupID,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go m.run(ctx)
	return m
}

func (m *lagMonitor) run(ctx context.Context) {
	defer close(m.done)

	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	m.refresh(ctx)

	for {
		select {
		case <-ticker.C:
			m.refresh(ctx)
		case <-m.stop:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (m *lagMonitor) refresh(ctx context.Context) {
	lag, err := GroupLag(ctx, m.groupID, Topic)
	if err != nil {
		return
	}
	m.mu.Lock()
	m.lag = lag
	m.mu.Unlock()
}

func (m *lagMonitor) lagExceeded() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lag > m.maxLag
}

func (m *lagMonitor) currentLag() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lag
}

func (m *lagMonitor) close() {
	close(m.stop)
	<-m.done
}

func (m *lagMonitor) wait(ctx context.Context) error {
	for {
		if !m.lagExceeded() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
