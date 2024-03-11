package taskq_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yarcat/taskq"
)

type MyTask struct {
	Phrase   string
	Duration time.Duration
}

// MyHandler contains the business logic for handling MyTask.
// The following fields are given as an example. In real life, you may want to
// use a logger, a database connection, another task queue, etc.
type MyHandler struct {
	Print func(string) error
	Sleep func(time.Duration)
}

func (h MyHandler) Run(ctx context.Context, task MyTask) error {
	h.Sleep(task.Duration)
	if err := h.Print(task.Phrase); err != nil {
		return fmt.Errorf("printing: %w", err)
	}
	return nil
}

func stopAfterIterations(n int64) func() bool {
	return func() bool { return atomic.AddInt64(&n, -1) >= 0 }
}

func Example() {
	ctx := context.Background()

	c := redis.NewClient(&redis.Options{
		Addr: "172.21.169.52:6379",
		DB:   12, // Use a different DB for each task queue.
	})

	h := &MyHandler{
		Print: func(s string) error {
			_, err := fmt.Println(s)
			return err
		},
		Sleep: time.Sleep,
	}

	var wg sync.WaitGroup
	wg.Add(1)

	q := taskq.New(ctx, "my", c, h.Run,
		taskq.WithProcessNext(stopAfterIterations(2)),
		taskq.WithNotifyStopped(wg.Done),
	)
	q.Add(ctx, MyTask{"Sleep 1", 1 * time.Second})
	q.Add(ctx, MyTask{"Sleep 2", 2 * time.Second})

	wg.Wait()
	// Output:
	// Sleep 1
	// Sleep 2
}
