package node

import (
	"context"
)

type KeyVal struct {
	Key   interface{}
	Value interface{}
}

type Record interface {
	KeyVal() KeyVal
}

type SourceBuilder interface {
	Name() string
	Info() map[string]string
	SourceType() string
	Build() (Source, error)
}

type SinkBuilder interface {
	NodeBuilder
	Name() string
	ID() int32
	Info() map[string]string
	SinkType() string
}

type Source interface {
	Run(ctx context.Context, kIn, vIn []byte) (kOut, vOut interface{}, err error)
	Name() string
	Close()
}

type Sink interface {
	Node
	Name() string
	Close() error
}

type mockSourceBuilder struct {
	source Source
}

func (mockSourceBuilder) Name() string {
	return `mockSource`
}

func (mockSourceBuilder) Info() map[string]string {
	return make(map[string]string)
}

func (mockSourceBuilder) SourceType() string {
	return `mock_source`
}

func (m *mockSourceBuilder) Build() (Source, error) {
	return m.source, nil
}

type mockSource struct {
	fn func(ctx context.Context, kIn, vIn []byte) (kOut, vOut interface{}, err error)
}

func (m mockSource) Run(ctx context.Context, kIn, vIn []byte) (kOut, vOut interface{}, err error) {
	return m.fn(ctx, kIn, vIn)
}

func (mockSource) Name() string {
	return `mock_source`
}

func (mockSource) Close() {}
