package join

/*import (
	"context"
	"fmt"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/k-stream/store"
	"sync"
)

type GlobalTableStarJoiner struct {
	//Topic string
	Joins []GlobalTableJoiner
}

func (j *GlobalTableStarJoiner) Join(ctx context.Context, key interface{}, leftVal interface{}) (joinedVal interface{}, err error) {

	wg := &sync.WaitGroup{}

	for _, join := range j.Joins{

	}


	return valJoined, nil

}

func (j *GlobalTableJoiner) Process(ctx context.Context, key interface{}, value interface{}) (interface{}, interface{}, error) {
	v, err := j.Join(ctx, key, value)
	return key, v, err
}

func (j *GlobalTableJoiner) Name() string {
	return j.Store
}

func (j *GlobalTableJoiner) JoinType() string {
	return `GlobalTableJoiner`
}*/
