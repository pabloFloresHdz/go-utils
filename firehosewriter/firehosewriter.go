package firehosewriter

import (
	"context"
	"errors"
	"io"
	"log"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	"github.com/pabloFloresHdz/go-utils/bgrunner"
	"github.com/pabloFloresHdz/go-utils/retry"
)

var ErrMaxMsgSizeExceeded = errors.New("exceeded max firehose message size")

const maxMsgSize = 1000*1024 - 1

type Opts struct {
	BacklogSize         int
	FlushInterval       time.Duration
	MaxRetries          int
	InitialRetryBackoff time.Duration
}

type firehoseBuffer struct {
	firehoseClient      firehoseiface.FirehoseAPI
	stream              string
	ctx                 context.Context
	backlogSize         int
	records             []*firehose.Record
	initialRetryBackoff time.Duration
	maxRetries          int
}

func (f *firehoseBuffer) Append(val interface{}) {
	if rec, ok := val.(*firehose.Record); ok {
		f.records = append(f.records, rec)
		return
	}
	log.Printf("unknown record type, expected *firehose.Record, got %s \n", reflect.TypeOf(val))
}

func (f *firehoseBuffer) IsReadyToFlush() bool {
	return len(f.records) >= f.backlogSize
}

func (f *firehoseBuffer) IsEmpty() bool {
	return len(f.records) == 0
}

func (f *firehoseBuffer) Flush() error {
	var (
		records  = f.records
		err      error
		retryCfg = retry.DefaultConfig
	)
	retryCfg.InitialBackoff = f.initialRetryBackoff
	retryCfg.Retries = f.maxRetries

	err = retry.Function(func() error {
		rs, err := f.firehoseClient.PutRecordBatchWithContext(f.ctx, &firehose.PutRecordBatchInput{
			DeliveryStreamName: &f.stream,
			Records:            records,
		})

		if err, ok := err.(awserr.Error); ok {
			if err.Code() == firehose.ErrCodeResourceNotFoundException {
				return retry.StopRetry(err)
			}
		}

		if rs.FailedPutCount != nil && *rs.FailedPutCount > 0 {
			records = getRetryableRecords(records, rs.RequestResponses)
			return err
		}

		return nil
	}, retryCfg)

	return err
}

func getRetryableRecords(input []*firehose.Record, output []*firehose.PutRecordBatchResponseEntry) (toRetry []*firehose.Record) {
	for i, record := range output {
		if record.ErrorCode != nil && *record.ErrorCode != "" {
			toRetry = append(toRetry, input[i])
		}
	}

	return
}

type firehoseWriter struct {
	data chan<- interface{}
	*bgrunner.Runner
}

// New returns an io.WriteCloser that writes to a kinesis firehose stream. Additionally, an associated channel is returned that signals when all the data in
// the underlying buffer has been written successfully. The best use for this channel is to wait before shutting down, to avoid having missing data in the stream
func New(ctx context.Context, firehoseClient firehoseiface.FirehoseAPI, stream string, opts Opts) (io.WriteCloser, <-chan struct{}) {
	data := make(chan interface{}, opts.BacklogSize)
	runner, doneChan := bgrunner.New(data, &firehoseBuffer{
		firehoseClient:      firehoseClient,
		stream:              stream,
		ctx:                 ctx,
		backlogSize:         opts.BacklogSize,
		maxRetries:          opts.MaxRetries,
		initialRetryBackoff: opts.InitialRetryBackoff,
	}, opts.FlushInterval)
	writer := &firehoseWriter{
		data:   data,
		Runner: runner,
	}

	go writer.Run(ctx)

	return writer, doneChan
}

func (f *firehoseWriter) Write(p []byte) (n int, err error) {
	if len(p) > maxMsgSize {
		return 0, ErrMaxMsgSizeExceeded
	}
	f.data <- &firehose.Record{Data: append(p, '\n')}
	return len(p) + 1, nil
}

func (f *firehoseWriter) Close() error {
	close(f.data)
	return nil
}
