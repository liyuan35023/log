package client

import (
	"context"
	"errors"
	"github.com/liyuan35023/utils/hbase/conf"
	"github.com/liyuan35023/utils/hbase/model"
	"sync"

	"github.com/Shopify/sarama"
)

type KafkaClient struct {
	brokers   []string
	config    *sarama.Config
	mesgQueue chan *sarama.ProducerMessage
	waitGroup *sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	topic     string
}

func NewKafkaClient(kafkaConf *conf.KafkaConf) *KafkaClient {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	ctx, cancel := context.WithCancel(context.Background())

	return &KafkaClient{
		brokers:   kafkaConf.Brokers,
		config:    config,
		mesgQueue: make(chan *sarama.ProducerMessage, kafkaConf.QueueSize),
		waitGroup: new(sync.WaitGroup),
		ctx:       ctx,
		cancel:    cancel,
		topic:     kafkaConf.WriteTopic,
	}
}

func (op *KafkaClient) messageProducerRoutine() {
	defer op.waitGroup.Done()
	producer, err := sarama.NewSyncProducer(op.brokers, op.config)

	if err != nil {
		// todo: error log.
		return
	}
	defer producer.Close()

	for mesg := range op.mesgQueue {
		_, _, err := producer.SendMessage(mesg)
		if err != nil {
			// todo: send back, warning ?
			op.mesgQueue <- mesg
		}
	}
}

func (op *KafkaClient) Insert(put *model.Put, succName string) error {
	return op.mutate((*model.Mutate)(put), model.MUTATE_PUT, succName)
}

func (op *KafkaClient) Remove(del *model.Delete, succName string) error {
	return op.mutate((*model.Mutate)(del), model.MUTATE_DELETE, succName)
}

func (op *KafkaClient) mutate(mut *model.Mutate, mutateType int, succName string) error {
	msg, err := model.GenerateKafkaMessage(op.topic, mut, mutateType, succName)
	if err != nil {
		return err
	}
	select {
	case <-op.ctx.Done():
		return errors.New("[HBaseErr] kafka message queue channel closed")
	case op.mesgQueue <- msg:
		return nil
	}
}

// Close finalize resource, like channel, wait goroutine finish.
// when terminate program, must call Kafka Close method.
func (op *KafkaClient) Close() {
	// stop all goroutine
	close(op.mesgQueue)

	// wait all message consumed.
	op.waitGroup.Wait()
}
