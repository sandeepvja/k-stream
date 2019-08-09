package store

type DiscoveryService struct {
}

func NewStoreDiscoveryService() *DiscoveryService {
	return &DiscoveryService{}
}

func (sd *DiscoveryService) stores() []Store {
	panic(`implement me`)
}

func (sd *DiscoveryService) store() Store {
	panic(`implement me`)
}
