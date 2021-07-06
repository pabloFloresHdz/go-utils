package bgrunner

import (
	"context"
	"log"
	"time"
)

type Runner struct {
	done          chan struct{}
	data          <-chan interface{}
	buffer        Buffer
	flushInterval time.Duration
}

type Buffer interface {
	Append(interface{})
	IsReadyToFlush() bool
	IsEmpty() bool
	Flush() error
}

// New returns a new background runner. This background runner will listen to the data receiver channel and will
// append the data to the provided buffer. The runner will check if the buffer has been filled completely before flushing
// it. The buffer will be flushed automatically after the `flushInterval` has been exceeded.
func New(receiverChannel <-chan interface{}, buf Buffer, flushInterval time.Duration) (*Runner, <-chan struct{}) {
	done := make(chan struct{})
	return &Runner{
		done:          done,
		data:          receiverChannel,
		buffer:        buf,
		flushInterval: flushInterval,
	}, done
}

// Run will start pushing data to the buffer as it is received. The provided context can be used to shut down the runner
func (r *Runner) Run(ctx context.Context) {
	var (
		flushTicker = time.NewTicker(r.flushInterval)
		closing     bool
	)
	defer flushTicker.Stop()
	defer close(r.done)
	for {
		select {
		// receive data flow
		case data := <-r.data:
			r.buffer.Append(data)
			// flush if buffer is full or if the runner is closing and the data channel is empty
			if r.buffer.IsReadyToFlush() || (closing && len(r.data) == 0) {
				if err := r.buffer.Flush(); err != nil {
					log.Printf("error flushing buffer %v", err)
				}
			}
			// clean up and stop running if closing and there is no pending data
			if closing && len(r.data) == 0 {
				return
			}
		// flush interval exceeded flow
		case <-flushTicker.C:
			if err := r.buffer.Flush(); err != nil {
				log.Printf("error flushing buffer %v", err)
			}
		// cancelation flow
		case <-ctx.Done():
			closing = true
			log.Println("Canceled by context. Flushing pending records")
			if !r.buffer.IsEmpty() {
				r.buffer.Flush()
			}
			// if the data channel is empty, clean up and stop running
			if len(r.data) == 0 {
				return
			}
		}
	}
}
