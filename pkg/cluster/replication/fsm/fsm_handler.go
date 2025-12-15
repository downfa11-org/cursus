package fsm

func (f *BrokerFSM) storeBroker(id string, broker *BrokerInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.brokers[id] = broker
}

func (f *BrokerFSM) removeBroker(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.brokers, id)
}

func (f *BrokerFSM) storePartitionMetadata(key string, metadata *PartitionMetadata) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.partitionMetadata[key] = metadata
}
