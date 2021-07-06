package sqswrapper

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/google/uuid"
	"github.com/pabloFloresHdz/go-utils/retry"
)

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

var irrecoverableErrors = []string{
	sqs.ErrCodeQueueDoesNotExist, sqs.ErrCodeTooManyEntriesInBatchRequest, sqs.ErrCodeInvalidAttributeName,
	sqs.ErrCodeQueueDeletedRecently, sqs.ErrCodeMessageNotInflight,
}

func init() {
	sort.Strings(irrecoverableErrors)
}

type Config struct {
	MaxRetries            int
	InitialBackoff        time.Duration
	AttributeNames        []string
	MessageAttributeNames []string
	MaxMessages           int64
	VisibilityTimeout     int64
	WaitTimeSecs          int64
	FifoQueue             bool
}

type Wrapper struct {
	client sqsiface.SQSAPI
	config Config
}

// Return a new wrapper with convenient methods to read data from SQS queues
func New(sqsClient sqsiface.SQSAPI, config Config) *Wrapper {
	return &Wrapper{
		client: sqsClient,
		config: config,
	}
}

// GetRecords read as much messages as configured and unmarshall them into `data`
func (w *Wrapper) GetRecords(ctx context.Context, queue string, data interface{}) (*sqs.ReceiveMessageOutput, error) {

	if rv := reflect.ValueOf(data); rv.Kind() != reflect.Ptr && reflect.Indirect(rv).Kind() != reflect.Array && reflect.Indirect(rv).Kind() != reflect.Slice {
		return nil, fmt.Errorf("data should be a pointer to array or slice, %s given", rv.Kind())
	}

	receiveMessageOutput, err := w.readFromQueue(ctx, queue)

	if err != nil {
		return nil, err
	}

	if err := decodeResponse(receiveMessageOutput, data); err != nil {
		return nil, fmt.Errorf("an error occurred while decoding sqs messages: %w", err)
	}

	return receiveMessageOutput, nil
}

func (w *Wrapper) getReceiveMessageInput(queue string) *sqs.ReceiveMessageInput {
	in := &sqs.ReceiveMessageInput{
		AttributeNames:        aws.StringSlice(w.config.AttributeNames),
		MessageAttributeNames: aws.StringSlice(w.config.MessageAttributeNames),
		QueueUrl:              &queue,
		MaxNumberOfMessages:   &w.config.MaxMessages,
		VisibilityTimeout:     &w.config.VisibilityTimeout,
		WaitTimeSeconds:       &w.config.WaitTimeSecs,
	}

	if w.config.FifoQueue {
		id, _ := uuid.NewRandom()
		in.ReceiveRequestAttemptId = aws.String(id.String())
	}

	return in
}

func (w *Wrapper) readFromQueue(ctx context.Context, queue string) (output *sqs.ReceiveMessageOutput, err error) {
	in := w.getReceiveMessageInput(queue)

	retryConfig := retry.DefaultConfig
	retryConfig.InitialBackoff = w.config.InitialBackoff
	retryConfig.Retries = w.config.MaxRetries
	retryConfig.InitialBackoff = w.config.InitialBackoff

	if err := retry.Function(func() error {
		output, err = w.client.ReceiveMessageWithContext(ctx, in)
		if awsErr, ok := err.(awserr.Error); ok {
			if sort.SearchStrings(irrecoverableErrors, awsErr.Code()) > len(irrecoverableErrors) {
				return retry.StopRetry(awsErr)
			}
		}

		return err
	}, retryConfig); err != nil {
		return nil, fmt.Errorf("error reading from queue %s: %w", queue, err)
	}

	return
}

func decodeResponse(rs *sqs.ReceiveMessageOutput, data interface{}) error {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.WriteByte('[')

	for i, message := range rs.Messages {
		buf.WriteString(*message.Body)
		if i < len(rs.Messages)-1 {
			buf.WriteByte(',')
		}
	}

	buf.WriteByte(']')
	return json.NewDecoder(buf).Decode(data)
}
