package stream

import (
	"github.com/pickme-go/k-stream/examples/example_1/encoders"
	"github.com/pickme-go/k-stream/k-stream"
	"github.com/pickme-go/k-stream/k-stream/task_pool"
	"github.com/pickme-go/log/v2"
	"github.com/pickme-go/metrics/v2"
	"os"
	"os/signal"
)

func Init() {

	log.StdLogger = log.Constructor.Log(
		log.WithLevel(`TRACE`),
		log.WithColors(true),
	)

	Logger := log.NewLog(
		log.WithLevel(`INFO`),
		log.WithColors(true),
	).Log()

	builderConfig := kstream.NewStreamBuilderConfig()
	builderConfig.BootstrapServers = []string{`localhost:9092`}
	builderConfig.ApplicationId = `k_stream_example_1`
	builderConfig.ConsumerCount = 1
	builderConfig.Host = `localhost:8100`
	builderConfig.AsyncProcessing = true
	//builderConfig.Store.StorageDir = `storage`
	builderConfig.Store.Http.Host = `:9002`
	builderConfig.ChangeLog.Enabled = false
	//builderConfig.ChangeLog.Buffer.Enabled = true
	//builderConfig.ChangeLog.Buffer.Size = 100
	//builderConfig.ChangeLog.ReplicationFactor = 3
	//builderConfig.ChangeLog.MinInSycReplicas = 2

	builderConfig.WorkerPool.Order = task_pool.OrderByKey
	builderConfig.WorkerPool.NumOfWorkers = 100
	builderConfig.WorkerPool.WorkerBufferSize = 10

	builderConfig.MetricsReporter = metrics.PrometheusReporter(metrics.ReporterConf{`streams`, `k_stream_test`, nil})
	builderConfig.Logger = Logger

	//builderConfig.Producer.Pool.NumOfWorkers = 1

	builder := kstream.NewStreamBuilder(builderConfig)

	builder.StoreRegistry().New(
		`account_detail_store`,
		encoders.KeyEncoder,
		encoders.AccountDetailsUpdatedEncoder)

	builder.StoreRegistry().New(
		`customer_profile_store`,
		encoders.KeyEncoder,
		encoders.CustomerProfileUpdatedEncoder)

	err := builder.Build(InitStreams(builder)...)
	if err != nil {
		log.Fatal(log.WithPrefix(`boot.boot.Init`, `error in stream building`), err)
	}

	synced := make(chan bool, 1)

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	stream := kstream.NewStreams(builder, kstream.NotifyOnStart(synced))
	go func() {
		select {
		case <-signals:
			stream.Stop()
		}
	}()

	if err := stream.Start(); err != nil {
		log.Fatal(log.WithPrefix(`boot.boot.Init`, `error in stream starting`), err)
	}

}

func InitStreams(builder *kstream.StreamBuilder) []kstream.Stream {

	transactionStream := initTransactionStream(builder)
	accountDetailTable := initAccountDetailTable(builder)
	customerProfileTable := initCustomerProfileTable(builder)

	accountCredited := AccountCredited{
		Upstream:             transactionStream,
		AccountDetailTable:   accountDetailTable,
		CustomerProfileTable: customerProfileTable,
		KeyEncoder:           encoders.KeyEncoder,
		MessageEncoder:       encoders.MessageEncoder,
	}
	accountCredited.Init()

	accountDebited := AccountDebited{
		Upstream:             transactionStream,
		AccountDetailTable:   accountDetailTable,
		CustomerProfileTable: customerProfileTable,
		KeyEncoder:           encoders.KeyEncoder,
		MessageEncoder:       encoders.MessageEncoder,
	}
	accountDebited.Init()

	return []kstream.Stream{transactionStream, accountDetailTable, customerProfileTable}
}
