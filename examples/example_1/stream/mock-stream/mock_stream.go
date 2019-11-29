package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/pickme-go/k-stream/admin"
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/k-stream/data"
	"github.com/pickme-go/k-stream/examples/example_1/encoders"
	"github.com/pickme-go/k-stream/examples/example_1/events"
	"github.com/pickme-go/k-stream/examples/example_1/stream"
	kstream "github.com/pickme-go/k-stream/k-stream"
	"github.com/pickme-go/k-stream/k-stream/offsets"
	"github.com/pickme-go/k-stream/k-stream/task_pool"
	"github.com/pickme-go/k-stream/producer"
	"github.com/pickme-go/log/v2"
	"math/rand"
	"os"
	"os/signal"
	"time"
)

func setupMockBuilders() *kstream.StreamBuilder {
	config := kstream.NewStreamBuilderConfig()
	topics := admin.NewMockTopics()
	kafkaAdmin := &admin.MockKafkaAdmin{
		Topics: topics,
	}

	if err := kafkaAdmin.CreateTopics(map[string]*admin.Topic{
		`transaction`: {
			Name:              "transaction",
			NumPartitions:     2,
			ReplicationFactor: 1,
		},
		`customer_profile`: {
			Name:              "customer_profile",
			NumPartitions:     2,
			ReplicationFactor: 1,
		},
		`account_detail`: {
			Name:              "account_detail",
			NumPartitions:     2,
			ReplicationFactor: 1,
		},
		`message`: {
			Name:              "message",
			NumPartitions:     2,
			ReplicationFactor: 1,
		},
	}); err != nil {
		log.Fatal(err)
	}

	prod := producer.NewMockProducer(topics)

	offsetManager := &offsets.MockManager{Topics: topics}
	config.DefaultBuilders.PartitionConsumer = consumer.NewMockPartitionConsumerBuilder(topics, offsetManager)
	config.DefaultBuilders.OffsetManager = offsetManager
	config.DefaultBuilders.KafkaAdmin = kafkaAdmin
	config.DefaultBuilders.Consumer = consumer.NewMockConsumerBuilder(topics)
	config.DefaultBuilders.Producer = func(configs *producer.Config) (i producer.Producer, e error) {
		return prod, nil
	}

	produceAccountDetails(prod)
	produceCustomerProfile(prod)
	go produceAccountCredited(prod)
	go produceAccountDebited(prod)
	go consumeMessageAndPrint(topics)

	config.BootstrapServers = []string{`localhost:9092`}
	config.ApplicationId = `k_stream_example_1`
	config.ConsumerCount = 1
	config.Host = `localhost:8100`
	config.AsyncProcessing = true
	//config.Store.StorageDir = `storage`
	config.Store.Http.Host = `:9002`
	config.ChangeLog.Enabled = false
	//config.ChangeLog.Buffer.Enabled = true
	//config.ChangeLog.Buffer.Size = 100
	//config.ChangeLog.ReplicationFactor = 3
	//config.ChangeLog.MinInSycReplicas = 2

	config.WorkerPool.Order = task_pool.OrderByKey
	config.WorkerPool.NumOfWorkers = 100
	config.WorkerPool.WorkerBufferSize = 10

	config.Logger = log.NewLog(
		log.WithLevel(`INFO`),
		log.WithColors(true),
	).Log()

	return kstream.NewStreamBuilder(config)
}

func main() {
	builder := setupMockBuilders()

	//mockBackend := backend.NewMockBackend(`mock_backend`, time.Duration(time.Second * 3600))
	//accountDetailMockStore := store.NewMockStore(`account_detail_store`, encoders.KeyEncoder(), encoders.AccountDetailsUpdatedEncoder(), mockBackend)
	//builder.StoreRegistry().Register(accountDetailMockStore)
	//
	//customerProfileMockStore := store.NewMockStore(`customer_profile_store`, encoders.KeyEncoder(), encoders.CustomerProfileUpdatedEncoder(), mockBackend)
	//builder.StoreRegistry().Register(customerProfileMockStore)

	builder.StoreRegistry().New(
		`account_detail_store`,
		encoders.KeyEncoder,
		encoders.AccountDetailsUpdatedEncoder)

	builder.StoreRegistry().New(
		`customer_profile_store`,
		encoders.KeyEncoder,
		encoders.CustomerProfileUpdatedEncoder)

	err := builder.Build(stream.InitStreams(builder)...)
	if err != nil{
		log.Fatal(`mock build failed`)
	}

	synced := make(chan bool, 1)

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	streams := kstream.NewStreams(builder, kstream.NotifyOnStart(synced))
	go func() {
		select {
		case <-signals:
			streams.Stop()
		}
	}()

	if err := streams.Start(); err != nil {
		log.Fatal(log.WithPrefix(`boot.boot.Init`, `error in stream starting`), err)
	}

	//produceRealData()
}

func produceAccountCredited(streamProducer producer.Producer) {

	for {
		key := rand.Int63n(100)
		event := events.AccountCredited{
			ID:        uuid.New().String(),
			Type:      `account_credited`,
			Timestamp: time.Now().UnixNano() / 1e6,
		}
		event.Body.AccountNo = key
		event.Body.TransactionId = rand.Int63n(10000)
		event.Body.Amount = 1000.00
		event.Body.Reason = `utility bill transfer`
		event.Body.DebitedFrom = 1111
		event.Body.CreditedAt = time.Now().UnixNano() / 1e6
		event.Body.Location = `Main Branch, City A`

		encodedKey, err := encoders.KeyEncoder().Encode(key)
		if err != nil {
			log.Error(err, event)
		}
		encodedVal, err := encoders.AccountCreditedEncoder().Encode(event)
		if err != nil {
			log.Error(err, event)
		}

		_, _, err = streamProducer.Produce(context.Background(), &data.Record{
			Key:       encodedKey,
			Value:     encodedVal,
			Topic:     `transaction`,
			Timestamp: time.Now(),
		})

		if err != nil {
			log.Error(err)
		}

		time.Sleep(time.Millisecond * 500)
	}

}

func produceAccountDebited(streamProducer producer.Producer) {

	for {
		key := rand.Int63n(100)
		event := events.AccountDebited{
			ID:        uuid.New().String(),
			Type:      `account_debited`,
			Timestamp: time.Now().UnixNano() / 1e6,
		}
		event.Body.AccountNo = key
		event.Body.TransactionId = rand.Int63n(10000)
		event.Body.Amount = 1000.00
		event.Body.Reason = `utility bill transfer`
		event.Body.CreditedTo = 2222
		event.Body.DebitedAt = time.Now().Unix()
		event.Body.Location = `Main Branch, City A`

		encodedKey, err := encoders.KeyEncoder().Encode(key)
		if err != nil {
			log.Error(err, event)
		}
		encodedVal, err := encoders.AccountDebitedEncoder().Encode(event)
		if err != nil {
			log.Error(err, event)
		}

		_, _, err = streamProducer.Produce(context.Background(), &data.Record{
			Key:       encodedKey,
			Value:     encodedVal,
			Topic:     `transaction`,
			Timestamp: time.Now(),
		})

		if err != nil {
			log.Error(err)
		}

		time.Sleep(time.Millisecond * 500)
	}
}

func produceAccountDetails(streamProducer producer.Producer) {
	for i := 1; i <= 100; i++ {
		key := int64(i)
		event := events.AccountDetailsUpdated{
			ID:        uuid.New().String(),
			Type:      `account_details_updated`,
			Timestamp: time.Now().UnixNano() / 1e6,
		}
		event.Body.AccountNo = key
		event.Body.AccountType = `Saving`
		event.Body.CustomerID = rand.Int63n(100)
		event.Body.Branch = `Main Branch, City A`
		event.Body.BranchCode = 1
		event.Body.UpdatedAt = time.Now().Unix()

		encodedKey, err := encoders.KeyEncoder().Encode(key)
		if err != nil {
			log.Error(err, event)
		}
		encodedVal, err := encoders.AccountDetailsUpdatedEncoder().Encode(event)
		if err != nil {
			log.Error(err, event)
		}

		_, _, err = streamProducer.Produce(context.Background(), &data.Record{
			Key:       encodedKey,
			Value:     encodedVal,
			Topic:     `account_detail`,
			Timestamp: time.Now(),
		})

		if err != nil {
			log.Error(err)
		}

		time.Sleep(time.Millisecond * 5)
	}
}

func produceCustomerProfile(streamProducer producer.Producer) {

	for i := 1; i <= 100; i++ {
		key := int64(i)
		event := events.CustomerProfileUpdated{
			ID:        uuid.New().String(),
			Type:      `customer_profile_updated`,
			Timestamp: time.Now().UnixNano() / 1e6,
		}
		event.Body.CustomerID = key
		event.Body.CustomerName = `Rob Pike`
		event.Body.NIC = `222222222v`
		event.Body.ContactDetails.Email = `example@gmail.com`
		event.Body.ContactDetails.Phone = `911`
		event.Body.ContactDetails.Address = `No 1, Lane 1, City A.`
		event.Body.DateOfBirth = `16th-Nov-2019`
		event.Body.UpdatedAt = time.Now().Unix()

		encodedKey, err := encoders.KeyEncoder().Encode(key)
		if err != nil {
			log.Error(err, event)
		}
		encodedVal, err := encoders.CustomerProfileUpdatedEncoder().Encode(event)
		if err != nil {
			log.Error(err, event)
		}

		_, _, err = streamProducer.Produce(context.Background(), &data.Record{
			Key:       encodedKey,
			Value:     encodedVal,
			Topic:     `customer_profile`,
			Timestamp: time.Now(),
		})

		if err != nil {
			log.Error(err)
		}

		time.Sleep(time.Millisecond * 5)
	}
}

func consumeMessageAndPrint(topics *admin.Topics) {
	mockConsumer := consumer.NewMockConsumer(topics)
	partitions, err := mockConsumer.Consume([]string{`message`}, rebalanceHandler{})
	if err != nil {
		log.Fatal(`consumer error `, err)
	}

	for p := range partitions {
		go func(pt consumer.Partition) {
			for record := range pt.Records() {
				log.Debug(fmt.Sprintf(`message was received to partition %v with offset %v `, record.Partition, record.Offset))
				m, err := encoders.MessageEncoder().Decode(record.Value)
				if err != nil {
					log.Error(err)
				}

				message, _ := m.(events.MessageCreated)
				fmt.Println(fmt.Sprintf(`received text message := %s`, message.Body.Text))
				log.Info(fmt.Sprintf(`received text message := %s`, message.Body.Text))
			}
		}(p)
	}
}

type rebalanceHandler struct {
}

func (r rebalanceHandler) OnPartitionRevoked(ctx context.Context, revoked []consumer.TopicPartition) error {
	return nil
}

func (r rebalanceHandler) OnPartitionAssigned(ctx context.Context, assigned []consumer.TopicPartition) error {
	return nil
}

func produceRealData() {
	config := producer.NewConfig()
	config.Logger = log.NewLog(
		log.WithLevel(`INFO`),
		log.WithColors(true),
	).Log()

	config.BootstrapServers = []string{`localhost:9092`}

	pro, err := producer.NewProducer(config)
	if err != nil {
		log.Fatal(err)
	}

	produceAccountDetails(pro)
	produceCustomerProfile(pro)
	go produceAccountCredited(pro)
	produceAccountDebited(pro)
}
