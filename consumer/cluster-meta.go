package consumer

import (
	"fmt"
	"github.com/pickme-go/k-stream/logger"
	"net/rpc"
	"sync"
)

func requestPartitionMeta(hostInfo map[string][]TopicPartition, logger logger.Logger) (err error) {
	wg := new(sync.WaitGroup)
	wg.Add(len(hostInfo))
	for host, tps := range hostInfo {
		go func(wg *sync.WaitGroup) {
			client, err := rpc.DialHTTP(`tcp`, host)
			if err != nil {
				logger.Fatal(`consumer.clusterMeta.requestPartitionMeta`, err)
			}
			defer client.Close()

			resp := new(RemotePartitionResp)
			if err := client.Call(`RemotePartitionDiscovery.Discover`, tps, resp); err != nil {
				logger.Fatal(`consumer.clusterMeta.requestPartitionMeta`, err)
			}

			logger.Info(`consumer.clusterMeta.requestPartitionMeta`, fmt.Sprintf(`waiting is done for %+v`, tps))
			wg.Done()
		}(wg)
	}
	wg.Wait()
	return nil
}
