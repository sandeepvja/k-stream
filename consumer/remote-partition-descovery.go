package consumer

import (
	"fmt"
	"github.com/pickme-go/k-stream/logger"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type RemotePartitionDiscovery struct {
	host         string
	log          logger.Logger
	partitionMap *PartitionMap
}

func newPartitionDiscovery(partitionMap *PartitionMap, host string, log logger.Logger) *RemotePartitionDiscovery {
	return &RemotePartitionDiscovery{
		partitionMap: partitionMap,
		host:         host,
		log:          log,
	}
}

type RemotePartitionResp struct {
	Partitions map[string]bool
}

func (pd *RemotePartitionDiscovery) Discover(tps []TopicPartition, resp *RemotePartitionResp) error {
	resp.Partitions = make(map[string]bool)
	wg := new(sync.WaitGroup)
	for _, tp := range tps {
		if !pd.partitionExist(tp) {
			resp.Partitions[tp.String()] = false
			continue
		}

		// if partition is not closed and removed from the map wait until it removed
		wg.Add(1)
		go pd.waitUntilClose(wg, tp, resp)
	}

	wg.Wait()
	return nil
}

func (pd *RemotePartitionDiscovery) partitionExist(tp TopicPartition) bool {
	_, ok := pd.partitionMap.partitions.Load(tp.String())
	return ok
}

func (pd *RemotePartitionDiscovery) waitUntilClose(wg *sync.WaitGroup, tp TopicPartition, tpResp *RemotePartitionResp) {
	ticker := time.NewTicker(100 * time.Microsecond)
	timeout := time.NewTimer(10 * time.Second)
LP:
	for {
		select {
		case <-ticker.C:
			if !pd.partitionExist(tp) {
				tpResp.Partitions[tp.String()] = false
				break LP
			}
		case <-timeout.C:
			tpResp.Partitions[tp.String()] = true
			break LP
		}
	}

	ticker.Stop()
	timeout.Stop()
	wg.Done()
}

func (pd *RemotePartitionDiscovery) startServer() error {
	if err := rpc.Register(pd); err != nil {
		return err
	}

	rpc.HandleHTTP()

	l, err := net.Listen("tcp", pd.host)
	if err != nil {
		return err
	}

	go func() {
		if err := http.Serve(l, nil); err != nil {
			pd.log.Fatal(`consumer.rpc`, `cannot start rpc server due to`, err)
		}
	}()

	pd.log.Info(`consumer.rpc`, fmt.Sprintf(`rpc server started on %s`, pd.host))
	return nil
}
