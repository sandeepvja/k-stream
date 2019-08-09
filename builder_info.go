package kstream

import (
	"bytes"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"os"
	"strings"
)

func printInfo(b *StreamBuilder) {
	data := [][]string{
		{"kstream.ApplicationId", fmt.Sprint(b.config.ApplicationId)},
		{"kstream.BootstrapServers", strings.Join(b.config.BootstrapServers, `, `)},
		{"kstream.ConsumerCount", fmt.Sprint(b.config.ConsumerCount)},
		{"kstream.Consumer.AutoCommitInterval", fmt.Sprint(b.config.Consumer.AutoCommitInterval)},
		{"kstream.Consumer.OffsetBegin", b.config.Consumer.OffsetBegin.String()},
		{"kstream.Consumer.AsyncProcessing", fmt.Sprint(b.config.AsyncProcessing)},
		{"kstream.Consumer.AutoCommitEnable", fmt.Sprint(b.config.Consumer.AutoCommitEnable)},
		{"kstream.Consumer.ClientId", fmt.Sprint(b.config.ApplicationId)},
		{"kstream.Consumer.EventChannelSize", fmt.Sprint(b.config.Consumer.EventChannelSize)},
		{"kstream.Consumer.GroupId", fmt.Sprint(b.config.ApplicationId)},
		{"kstream.Consumer.BootstrapServers", fmt.Sprint(b.config.BootstrapServers)},
		{"kstream.Consumer.FetchErrorBackOffMs", fmt.Sprint(b.config.Consumer.FetchErrorBackOffMs)},
		{"kstream.Consumer.FetchMinBytes", fmt.Sprint(b.config.Consumer.FetchMinBytes)},
		{"kstream.Consumer.FetchWaitMaxMs", fmt.Sprint(b.config.Consumer.FetchWaitMaxMs)},
		{"kstream.Consumer.HeartbeatIntervalMs", fmt.Sprint(b.config.Consumer.HeartbeatIntervalMs)},
		{"kstream.Consumer.SessionTimeoutMs", fmt.Sprint(b.config.Consumer.SessionTimeoutMs)},
		{"kstream.Consumer.MetadataMaxAgeMs", fmt.Sprint(b.config.Consumer.MetadataMaxAgeMs)},
		{``, ``},
		{"kstream.WorkerPool.NumOfWorkers (Per Partition)", fmt.Sprint(b.config.WorkerPool.NumOfWorkers)},
		{"kstream.WorkerPool.WorkerBufferSize (Per Partition)", fmt.Sprint(b.config.WorkerPool.WorkerBufferSize)},
		{"kstream.WorkerPool.Order", fmt.Sprint(b.config.WorkerPool.Order)},
		{``, ``},
		{"kstream.Producer.NumOfWorkers", fmt.Sprint(b.config.Producer.NumOfWorkers)},
		{"kstream.Producer.Idempotent", fmt.Sprint(b.config.Producer.Idempotent)},
		{"kstream.Producer.BatchNumMessages", fmt.Sprint(b.config.Producer.BatchNumMessages)},
		{"kstream.Producer.QueueBufferingMax", fmt.Sprint(b.config.Producer.QueueBufferingMax)},
		{"kstream.Producer.RequiredAcks", fmt.Sprint(b.config.Producer.RequiredAcks)},
		{"kstream.Producer.Retry", fmt.Sprint(b.config.Producer.Retry)},
		{"kstream.Producer.RetryMax", fmt.Sprint(b.config.Producer.RetryBackOff)},
		{``, ``},
		{"kstream.ChangeLog.Buffered.Enabled", fmt.Sprint(b.config.ChangeLog.Buffer.Enabled)},
		{"kstream.ChangeLog.BufferedSize", fmt.Sprint(b.config.ChangeLog.Buffer.Size)},
		{"kstream.ChangeLog.BufferedFlush", b.config.ChangeLog.Buffer.FlushInterval.String()},
		{"kstream.ChangeLog.MinInSycReplicas", fmt.Sprint(b.config.ChangeLog.MinInSycReplicas)},
		{"kstream.ChangeLog.ReplicationFactor", fmt.Sprint(b.config.ChangeLog.ReplicationFactor)},
		{"kstream.ChangeLog.Replicated", fmt.Sprint(b.config.ChangeLog.Replicated)},
		{"kstream.ChangeLog.Suffix", fmt.Sprint(b.config.ChangeLog.Suffix)},
		{``, ``},
		{"kstream.Store.Http.Host", fmt.Sprint(b.config.Store.Http.Host)},
	}

	data = append(data, []string{``})
	data = append(data, []string{`Stream configs`, ``})
	for topic, stream := range b.streams {

		b := new(bytes.Buffer)
		flowTable := tablewriter.NewWriter(b)
		flowData := [][]string{
			{`changeLog.Enabled`, fmt.Sprint(stream.changelog.enabled)},
		}
		if stream.changelog.enabled {
			flowData = append(flowData,
				[]string{`changeLog.Buffered`, fmt.Sprint(stream.changelog.buffer.enabled)},
				[]string{`changeLog.Buffer.Size`, fmt.Sprint(stream.changelog.buffer.size)},
				[]string{`changeLog.Buffer.Flush`, stream.changelog.buffer.flushInterval.String()},
				[]string{`changeLog.MinInSycReplicas`, fmt.Sprint(stream.changelog.topic.minInSycReplicas)},
				[]string{`changeLog.ReplicationFactor`, fmt.Sprint(stream.changelog.topic.replicationFactor)},
				[]string{`changeLog.Replicated`, fmt.Sprint(stream.changelog.replicated)},
				[]string{`changeLog.Suffix`, fmt.Sprint(stream.changelog.topic.suffix)},
			)
		}

		for _, v := range flowData {
			flowTable.SetColumnAlignment([]int{tablewriter.ALIGN_LEFT})
			flowTable.Append(v)
		}
		flowTable.Render()
		data = append(data, []string{topic, b.String()})
	}

	data = append(data, []string{``})
	data = append(data, []string{`Global table configs`, ``})
	for topic, t := range b.globalTables {

		b := new(bytes.Buffer)
		flowTable := tablewriter.NewWriter(b)

		tableData := [][]string{
			{`changeLog.Enabled`, fmt.Sprint(t.store.changelog.enabled)},
		}
		if t.store.changelog.enabled {
			tableData = append(tableData,
				[]string{`changeLog.Buffered`, fmt.Sprint(t.store.changelog.buffer.enabled)},
				[]string{`changeLog.Buffer.Size`, fmt.Sprint(t.store.changelog.buffer.size)},
				[]string{`changeLog.Buffer.Flush`, t.store.changelog.buffer.flushInterval.String()},
				[]string{`changeLog.MinInSycReplicas`, fmt.Sprint(t.store.changelog.topic.minInSycReplicas)},
				[]string{`changeLog.ReplicationFactor`, fmt.Sprint(t.store.changelog.topic.replicationFactor)},
				[]string{`changeLog.Replicated`, fmt.Sprint(t.store.changelog.replicated)},
				[]string{`changeLog.Suffix`, fmt.Sprint(t.store.changelog.topic.suffix)},
			)
		}

		for _, v := range tableData {
			flowTable.SetColumnAlignment([]int{tablewriter.ALIGN_LEFT})
			flowTable.Append(v)
		}
		flowTable.Render()
		data = append(data, []string{topic, b.String()})
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Config", "Value"})

	for _, v := range data {
		table.SetColumnAlignment([]int{tablewriter.ALIGN_LEFT})
		table.Append(v)
	}
	table.Render()
}
