package store

import (
	"github.com/pickme-go/k-stream/backend"
	"github.com/pickme-go/k-stream/k-stream/changelog"
	"time"
)

type storeOptions struct {
	changelog         changelog.Changelog
	changelogEnable   bool
	backend           backend.Backend
	backendBuilder    backend.Builder
	expiry            time.Duration
	buffered          bool
	bufferSize        int
	compactionEnabled bool
}

type Options func(config *storeOptions)

func (c *storeOptions) apply(options ...Options) {
	for _, opt := range options {
		opt(c)
	}
}

func ChangelogEnabled() Options {
	return func(config *storeOptions) {
		config.changelogEnable = true
	}
}

func WithChangelog(changelog changelog.Changelog) Options {
	return func(config *storeOptions) {
		config.changelog = changelog
		config.changelogEnable = true
	}
}

func Compacated() Options {
	return func(options *storeOptions) {
		options.compactionEnabled = true
	}
}

func Expire(d time.Duration) Options {
	return func(options *storeOptions) {
		options.expiry = d
	}
}

func Buffered(size int) Options {
	return func(options *storeOptions) {
		options.buffered = true
		options.bufferSize = size
	}
}

func WithBackend(backend backend.Backend) Options {
	return func(config *storeOptions) {
		config.backend = backend
	}
}

func WithBackendBuilder(builder backend.Builder) Options {
	return func(config *storeOptions) {
		config.backendBuilder = builder
	}
}
