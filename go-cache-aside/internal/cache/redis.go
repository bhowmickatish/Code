package cache

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func NewClusterClient(addrs []string) (redis.UniversalClient, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("redis cluster: no addresses configured")
	}

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: addrs,
	})
	if err := client.Ping(context.Background()).Err(); err != nil {
		client.Close()
		return nil, err
	}
	return client, nil
}
