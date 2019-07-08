package consumer

import "context"

type ReBalanceHandler interface {
	OnPartitionRevoked(ctx context.Context, revoked []TopicPartition)
	OnPartitionAssigned(ctx context.Context, assigned []TopicPartition)
}
