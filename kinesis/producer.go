package kinesis

import (
	"s3-kinesis-replay/validate"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/cenkalti/backoff"
	"github.com/sirupsen/logrus"
)

// Producer implements a replay producer that uses kinesis batch writes
// as the streaming mechanism
type Producer struct {
	backoff      *backoff.ExponentialBackOff
	bufferWindow time.Duration
	client       kinesisiface.KinesisAPI
	log          logrus.FieldLogger
	streamName   *string
	wg           *sync.WaitGroup
}

// NewProducer returns a new kinesis producer
func NewProducer(c *ProducerConfig) (*Producer, error) {
	// validate configuration
	err := validate.V.Struct(c)
	if err != nil {
		return nil, err
	}
	// define exponential backoff settings
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = c.BackoffInterval
	b.MaxInterval = c.BackoffMaxInterval
	// create new producer
	p := &Producer{
		backoff:      b,
		bufferWindow: c.BufferWindow,
		client:       c.Client,
		log:          c.Log,
		streamName:   &c.StreamName,
		wg:           &sync.WaitGroup{},
	}
	return p, nil
}

// bufferWithTimeOrCount continues adding incoming entries to the current batch until
// the it reaches the specified max size or the timeout is reached
func (p *Producer) bufferWithTimeOrCount(entries chan *kinesis.PutRecordsRequestEntry, first *kinesis.PutRecordsRequestEntry, max int, window time.Duration) []*kinesis.PutRecordsRequestEntry {
	batch := []*kinesis.PutRecordsRequestEntry{first}
	timeout := time.After(window)
	n := len(batch)
	for {
		select {
		case <-timeout:
			return batch
		case e, ok := <-entries:
			// exit if channel has closed
			if !ok {
				return batch
			}
			// add entry to batch
			batch = append(batch, e)
			n++
			if n >= max {
				return batch
			}
		}
	}
}

// Process incoming stream of kinesis entries by bulk writing to kinesis with
// error handling
func (p *Producer) process(entries chan *kinesis.PutRecordsRequestEntry) {
	// define resuble kinesis bulk parameters
	params := &kinesis.PutRecordsInput{
		StreamName: p.streamName,
	}
	// continuously read
	for e := range entries {
		// create batch
		batch := p.bufferWithTimeOrCount(entries, e, 500, p.bufferWindow)
		params.Records = batch

		// batch write to kinesis
		var output *kinesis.PutRecordsOutput
		var attempterr error
		p.backoff.Reset()
		for len(params.Records) > 0 {
			// batch write to kinesis
			err := backoff.Retry(func() error {
				output, attempterr = p.client.PutRecords(params)
				if attempterr != nil {
					p.log.WithError(attempterr).Warnln("kiensis attempt error")
				}
				return attempterr
			}, p.backoff)
			if err != nil {
				p.log.WithError(err).Fatalln("kinesis error")
			}
			// retry any individual records that failed due to throttling
			failed := []*kinesis.PutRecordsRequestEntry{}
			if output.FailedRecordCount != nil {
				for i, entry := range output.Records {
					if entry.ErrorCode != nil {
						failed = append(failed, params.Records[i])
						p.log.WithError(err).Warnln("kinesis record error")
					}
				}
			}

			// if throttling errors detected, pause briefly
			if l := len(failed); l > 0 {
				p.log.WithField("n", l).Warnln("scheduling retry of failed records")
				time.Sleep(p.backoff.NextBackOff())
			} else {
				p.log.WithField("n", len(params.Records)).Debugln("batch success")
			}
			params.Records = failed
		}
	}
	p.wg.Done()
}

// Stream returns a channel that accepts kinesis messages to replay which
// are buffered by time/count and written in bulk to kinesis
func (p *Producer) Stream() chan *kinesis.PutRecordsRequestEntry {
	entries := make(chan *kinesis.PutRecordsRequestEntry, 1000)
	p.wg.Add(1)
	go p.process(entries)
	return entries
}

// Wait will block until the producer has finished replaying all messages
func (p *Producer) Wait() {
	p.wg.Wait()
}

// ProducerConfig defines producer configuration settings
type ProducerConfig struct {
	BackoffInterval    time.Duration           `validate:"required"`
	BackoffMaxInterval time.Duration           `validate:"required"`
	BufferWindow       time.Duration           `validate:"required"`
	Client             kinesisiface.KinesisAPI `validate:"required"`
	Log                logrus.FieldLogger      `validate:"required"`
	StreamName         string                  `validate:"required"`
}

// NewProducerConfig returns a new ProducerConfig valueu with appropriate
// defaults
func NewProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		BackoffInterval:    time.Millisecond * 500,
		BackoffMaxInterval: time.Minute,
		BufferWindow:       time.Second * 10,
		Log:                logrus.WithField("package", "kinesis"),
	}
}
