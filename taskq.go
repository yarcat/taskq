package taskq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"runtime/debug"

	"github.com/redis/go-redis/v9"
)

type (
	options struct {
		processNext   func() bool
		notifyStarted func()
		notifyStopped func()
		processFailed func(string, error)
	}
	// OptionFunc provides an option setting function.
	OptionFunc func(*options)
)

// ProcessNext returns true if the next task should be processed.
func (o options) ProcessNext() bool {
	if o.processNext == nil {
		return true
	}
	return o.processNext()
}

// NotifyStarted calls the function that is called when the task queue starts.
func (o options) NotifyStarted() {
	if o.notifyStarted != nil {
		o.notifyStarted()
	}
}

// NotifyStopped calls the function that is called when the task queue stops.
func (o options) NotifyStopped() {
	if o.notifyStopped != nil {
		o.notifyStopped()
	}
}

// ProcessFailed calls the function that is called when a task fails to process.
func (o options) ProcessFailed(task string, err error) {
	if o.processFailed != nil {
		o.processFailed(task, err)
	}
}

// WithProcessNext provides an option to set the function that determines if the next task should be processed.
func WithProcessNext(f func() bool) OptionFunc {
	return func(o *options) { o.processNext = f }
}

// WithNotifyStarted provides an option to set the function that is called when the task queue starts.
func WithNotifyStarted(f func()) OptionFunc {
	return func(o *options) { o.notifyStarted = f }
}

// WithNotifyStopped provides an option to set the function that is called when the task queue stops.
func WithNotifyStopped(f func()) OptionFunc {
	return func(o *options) { o.notifyStopped = f }
}

// WithProcessFailed provides an option to set the function that is called when a task fails to process.
func WithProcessFailed(f func(string, error)) OptionFunc {
	return func(o *options) { o.processFailed = f }
}

type (
	// TaskHandlerFunc provides a function that handles a task.
	TaskHandlerFunc[T any] func(context.Context, T) error

	// TaskQ provides a queue for tasks.
	TaskQ[T any] struct {
		queueKey      string
		processingKey string
		c             *redis.Client
		f             func(context.Context, T) error
		g             slog.Attr
		opt           options
	}
)

// NewTaskQ returns a new task queue.
func NewTaskQ[T any](ctx context.Context, key string, c *redis.Client, f TaskHandlerFunc[T], opts ...OptionFunc) *TaskQ[T] {
	var opt options
	for _, o := range opts {
		o(&opt)
	}

	queueKey := key
	processingKey := key + "-processing"

	g := slog.Group("taskq",
		slog.String("queue-key", key),
		slog.String("processing-key", key+"-processing"))

	tq := &TaskQ[T]{
		queueKey:      queueKey,
		processingKey: processingKey,
		c:             c,
		f:             f,
		g:             g,
		opt:           opt,
	}
	go tq.processLoop(ctx)
	return tq
}

// Add adds a task to the queue.
func (tq *TaskQ[T]) Add(ctx context.Context, x T) error {
	data, err := json.Marshal(x)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", tq.queueKey, err)
	}
	return tq.c.LPush(ctx, tq.queueKey, string(data)).Err()
}

func (tq *TaskQ[T]) processLoop(ctx context.Context) {
	defer tq.opt.NotifyStopped()
	tq.opt.NotifyStarted()
	for ctx.Err() == nil && tq.opt.ProcessNext() {
		task, err := tq.c.LIndex(ctx, tq.processingKey, 0).Result()
		if err == redis.Nil {
			task, err = tq.c.BLMove(ctx, tq.queueKey, tq.processingKey, "right", "left", 0).Result()
		}
		var t T
		if err == nil {
			err = json.Unmarshal([]byte(task), &t)
		}
		if err == nil {
			err = tq.processTask(ctx, t)
		}
		if err == nil {
			slog.LogAttrs(ctx, slog.LevelInfo, "Processed task", tq.g,
				slog.String("task", task))
		} else {
			slog.LogAttrs(ctx, slog.LevelError, "Failed to process task", tq.g,
				slog.String("task", task),
				slog.String("error", err.Error()))
			tq.opt.ProcessFailed(task, err)
		}
		if err = tq.c.LRem(ctx, tq.processingKey, -1, task).Err(); err != nil {
			slog.LogAttrs(ctx, slog.LevelError, "Failed to remove task", tq.g,
				slog.String("task", task),
				slog.String("error", err.Error()))
		} else {
			slog.LogAttrs(ctx, slog.LevelInfo, "Removed task", tq.g,
				slog.String("task", task))
		}
	}
}

func (tq *TaskQ[T]) processTask(ctx context.Context, t T) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		}
	}()
	err = tq.f(ctx, t)
	return err
}
