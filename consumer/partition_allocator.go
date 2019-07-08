/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package consumer

// Allocation current upstream TopicPartition allocation
type Allocation struct {
	Assigned []TopicPartition
	Removed  []TopicPartition
}

type PartitionAllocator struct {
	currentAllocation []TopicPartition
	newAllocation     Allocation
}

func newPartitionAllocator() *PartitionAllocator {
	return &PartitionAllocator{
		currentAllocation: make([]TopicPartition, 0),
		newAllocation: Allocation{
			Assigned: make([]TopicPartition, 0),
			Removed:  make([]TopicPartition, 0),
		},
	}
}

func (b *PartitionAllocator) assign(newAllocation []TopicPartition) {

	b.clear()
	// first time allocation
	if len(b.currentAllocation) < 1 {
		b.currentAllocation = newAllocation
		b.newAllocation.Assigned = newAllocation
		return
	}

	// check new items
	for _, tp := range newAllocation {
		if !exist(tp, b.currentAllocation) {
			b.newAllocation.Assigned = append(b.newAllocation.Assigned, tp)
		}
	}

	// check Removed items
	for _, oldTp := range b.currentAllocation {
		if !exist(oldTp, newAllocation) {
			b.newAllocation.Removed = append(b.newAllocation.Removed, oldTp)
		}
	}

	b.currentAllocation = newAllocation
}

func (b *PartitionAllocator) clear() {
	b.newAllocation = Allocation{}
}

func (b *PartitionAllocator) allocations() Allocation {
	return b.newAllocation
}

func exist(key TopicPartition, arr []TopicPartition) bool {
	for _, ele := range arr {
		if ele == key {
			return true
		}
	}
	return false
}
