package store

import (
	"context"
	"fmt"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/changelog"
	"github.com/pickme-go/k-stream/logger"
)

type RecoverableStore interface {
	Store
	Recover(ctx context.Context) error
}

type recoverableStore struct {
	Store
	changelog changelog.Changelog
}

func (s *recoverableStore) Recover(ctx context.Context) error {

	logger.DefaultLogger.Info(`k-stream.store.recoverableStore`,
		fmt.Sprintf(`recovering from store [%s]...`, s.Name()))
	var c int
	records, err := s.changelog.ReadAll(ctx)
	if err != nil {
		return errors.WithPrevious(err, `k-stream.store.recoverableStore`,
			fmt.Sprintf(`cannot recover data for store [%s]`, s.Name()))
	}

	for _, record := range records {
		if err := s.Backend().Set(record.Key, record.Value, 0); err != nil {
			return err
		}
	}

	logger.DefaultLogger.Info(`k-stream.store.recoverableStore`,
		fmt.Sprintf(`[%d] records recovered for store [%s]...`, c, s.Name()))

	return nil
}

func (s *recoverableStore) String() string {
	return fmt.Sprintf("Backend: %s\nChangelogInfo: %s", s.Backend().Name(), s.changelog)
}
