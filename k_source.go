package kstream

import (
	"context"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/encoding"
	"github.com/pickme-go/k-stream/internal/node"
)

type kSource struct {
	keyEncoder        encoding.Encoder
	valEncoder        encoding.Encoder
	keyEncoderBuilder encoding.Builder
	valEncoderBuilder encoding.Builder
	name              string
	topic             string
}

type kSourceBuilder struct {
	keyEncoderBuilder encoding.Builder
	valEncoderBuilder encoding.Builder
	name              string
	topic             string
	info              map[string]string
}

func (b *kSourceBuilder) Build() (node.Source, error) {

	return &kSource{
		name:       b.name,
		topic:      b.topic,
		keyEncoder: b.keyEncoderBuilder(),
		valEncoder: b.valEncoderBuilder(),
	}, nil
}

func (b *kSourceBuilder) Name() string {
	return b.name
}

func (b *kSourceBuilder) SourceType() string {
	return `kafka`
}

func (b *kSourceBuilder) Info() map[string]string {
	return b.info
}

func (s *kSource) Name() string {
	return s.name
}

func (s *kSource) Run(ctx context.Context, kIn, vIn []byte) (kOut, vOut interface{}, err error) {
	return s.decodeRecord(kIn, vIn)
}

func (s *kSource) decodeRecord(key []byte, val []byte) (interface{}, interface{}, error) {
	k, err := s.keyEncoder.Decode(key)
	if err != nil {
		return nil, nil, errors.WithPrevious(err, `k-stream.streamProcessor`, `key decode error`)
	}

	v, err := s.valEncoder.Decode(val)
	if err != nil {
		return nil, nil, errors.WithPrevious(err, `k-stream.streamProcessor`, `value decode error`)
	}

	return k, v, nil
}

func (s *kSource) Close() {}

func (s *kSource) Next() bool {
	return true
}

func (s *kSource) Type() node.Type {
	return node.TypeSource
}
