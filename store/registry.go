package store

import (
	"fmt"
	"github.com/pickme-go/k-stream/encoding"
	"github.com/pickme-go/k-stream/logger"
	"github.com/pickme-go/metrics"
	"sync"
)

type Registry interface {
	Register(store Store)
	New(name string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...Options) Store
	Store(name string) Store
	List() []string
}

type registry struct {
	Stores            map[string]Store
	StateStores       map[string]StateStore
	mu                *sync.Mutex
	applicationId     string
	storeBuilder      Builder
	stateStoreBuilder StateStoreBuilder
}

type RegistryConfig struct {
	Host              string
	applicationId     string
	StoreBuilder      Builder
	StateStoreBuilder StateStoreBuilder
	Logger            logger.Logger
	MetricsReporter   metrics.Reporter
}

func NewRegistry(config *RegistryConfig) Registry {
	reg := &registry{
		Stores:            make(map[string]Store, 0),
		StateStores:       make(map[string]StateStore, 0),
		mu:                &sync.Mutex{},
		applicationId:     config.applicationId,
		stateStoreBuilder: config.StateStoreBuilder,
		storeBuilder:      config.StoreBuilder,
	}

	MakeEndpoints(config.Host, reg)

	return reg
}

func (r *registry) Register(store Store) {
	name := store.Name()
	if _, ok := r.Stores[name]; ok {
		logger.DefaultLogger.Fatal(`k-stream.Store.Registry`,
			fmt.Sprintf(`store [%s] already exist`, name))
	}

	r.Stores[name] = store
}

func (r *registry) New(name string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...Options) Store {
	if _, ok := r.Stores[name]; ok {
		logger.DefaultLogger.Fatal(`k-stream.Store.Registry`, fmt.Sprintf(`store [%s] already exist`, name))
	}

	s, err := r.storeBuilder(name, keyEncoder, valEncoder, options...)
	if err != nil {
		logger.DefaultLogger.Fatal(`k-stream.Store.Registry`, err)
	}

	r.Stores[name] = s

	return r.Stores[name]
}

func (r *registry) Store(name string) Store {

	r.mu.Lock()
	defer r.mu.Unlock()

	store, ok := r.Stores[name]
	if !ok {
		logger.DefaultLogger.Error(`k-stream.Store.Registry`, fmt.Sprintf(`unknown store [%s]`, name))
	}

	return store
}

func (r *registry) List() []string {
	var list []string

	for name := range r.Stores {
		list = append(list, name)
	}

	return list
}
