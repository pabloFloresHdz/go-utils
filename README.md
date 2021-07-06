# go-utils
## Collection of utilities used in different projects

## Packages included
- `bgrunner`: Run a process in the background that asynchronously flushes data from one end to another
- `firehosewriter`: Implementation of io.WriteCloser that writes data asynchronously to kinesis firehose
- `retry`: Implementation of a function retrier. Includes different parameters to control the number of retries and backoff to wait between retries
-  `sqswrapper`: Utility to fetch messages from sqs queues and automatically unmarshall the content of the messages

## How to use 

### bgrunner

The best way to see this package in use is looking into the firehosewriter implementation. In summary, one must implement the `Buffer` interface so that the runner can asynchronously push data. A constructor function may be used to start the background worker and the completion channel may be returned or handled by the implementation

## firehosewriter

Use it as a normal writer implementation, but be sure to close it to avoid memory leaks. Also, be sure to stop pushing data using the `Write` function before calling close and before cancelling the context, as a panic error could be thrown when writing data while closing the data channel.

## sqswrapper

After creating the wrapper you can use it to read json data like this

```
type Model struct {
    Prop1 int
    Prop2 string
    Prop3 float64
}

wrapper := sqswrapper.New(...)
var records []Model
rawRecords, err := wrapper.GetRecords(context.Background(), "queue-name", &records)
if err != nil {
    //handle error
}
// do something with the records received
// delete messages from sqs using rawRecords to create the delete requests
```
