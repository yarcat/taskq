package taskq

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/go-redis/redismock/v9"
	"github.com/google/go-cmp/cmp"
	"github.com/redis/go-redis/v9"
)

func runOnce() func() bool {
	once := make(chan bool, 1)
	once <- true
	return func() bool {
		v := <-once
		once <- false
		return v
	}
}

func runNever() func() bool {
	return func() bool { return false }
}

type (
	ctrlOptions struct {
		qOpts []OptionFunc
		err   func() error
	}
	ctrlOptionFunc func(*ctrlOptions)
)

func with(opts ...OptionFunc) ctrlOptionFunc {
	return func(o *ctrlOptions) { o.qOpts = append(o.qOpts, opts...) }
}

func withErr(err error) ctrlOptionFunc {
	return func(o *ctrlOptions) { o.err = func() error { return err } }
}

func withPanic(err error) ctrlOptionFunc {
	return func(o *ctrlOptions) { o.err = func() error { panic(err) } }
}

type qCtrl[Data any] struct {
	tq      *TaskQ[Data]
	unblock chan struct{}
	stopped chan struct{}
	out     chan Data
	outErr  chan error
}

func newCtrl[Data any](c *redis.Client, opts ...ctrlOptionFunc) qCtrl[Data] {
	var o ctrlOptions
	for _, opt := range opts {
		opt(&o)
	}
	out := make(chan Data, 1)
	outErr := make(chan error, 1)
	added, stopped := make(chan struct{}), make(chan struct{})
	tq := New(
		context.Background(),
		"test-queue",
		c,
		func(ctx context.Context, t Data) error {
			out <- t
			if o.err == nil {
				return nil
			}
			return o.err()
		},
		append([]OptionFunc{
			WithProcessNext(runOnce()),
			WithNotifyStarted(func() { <-added }),
			WithNotifyStopped(func() { close(stopped) }),
			WithProcessFailed(func(_ string, err error) { outErr <- err }),
		}, o.qOpts...)...,
	)
	return qCtrl[Data]{
		tq:      tq,
		unblock: added,
		stopped: stopped,
		out:     out,
		outErr:  outErr,
	}
}

func (q qCtrl[Data]) UnblockProcess()                       { close(q.unblock) }
func (q qCtrl[Data]) WaitStopped()                          { <-q.stopped }
func (q qCtrl[Data]) Data() Data                            { return <-q.out }
func (q qCtrl[Data]) Add(ctx context.Context, t Data) error { return q.tq.Add(ctx, t) }
func (q qCtrl[Data]) Processed() Data                       { return <-q.out }
func (q qCtrl[Data]) WaitProcessError() error               { return <-q.outErr }

func TestTaskQ(t *testing.T) {
	type Data struct {
		A int `json:"a,omitempty"`
	}

	t.Run("add and process", func(t *testing.T) {
		c, m := redismock.NewClientMock()
		defer c.Close()

		m.ExpectLPush("test-queue", `{"a":123}`).SetVal(1)
		m.ExpectLIndex("test-queue-processing", 0).RedisNil()
		m.ExpectBLMove("test-queue", "test-queue-processing", "right", "left", 0).SetVal(`{"a":123}`)
		m.ExpectLRem("test-queue-processing", -1, `{"a":123}`).SetVal(1)

		ctrl := newCtrl[Data](c)
		if gotErr := ctrl.Add(context.Background(), Data{A: 123}); gotErr != nil {
			t.Errorf("Add() got err = %v, want nil", gotErr)
		}
		ctrl.UnblockProcess()
		if diff := cmp.Diff(ctrl.Processed(), Data{A: 123}); diff != "" {
			t.Errorf("unexpected task: (-got +want)\n%s", diff)
		}
		ctrl.WaitStopped()
		if err := m.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("add fail", func(t *testing.T) {
		var errTest = errors.New("test error")

		c, m := redismock.NewClientMock()
		defer c.Close()

		m.ExpectLPush("test-queue", `{"a":123}`).SetErr(errTest)

		ctrl := newCtrl[Data](c, with(WithProcessNext(runNever())))
		if gotErr := ctrl.Add(context.Background(), Data{A: 123}); !errors.Is(gotErr, errTest) {
			t.Errorf("Add() got err = %v, want %v", gotErr, errTest)
		}
		ctrl.UnblockProcess()
		ctrl.WaitStopped()
		if err := m.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("process fail", func(t *testing.T) {
		var errTest = errors.New("test error")

		for _, tc := range []struct {
			name     string
			errOpt   ctrlOptionFunc
			checkErr func(error) bool
		}{
			{name: "error", errOpt: withErr(errTest), checkErr: func(err error) bool { return errors.Is(err, errTest) }},
			{name: "panic", errOpt: withPanic(errTest), checkErr: func(err error) bool {
				if err == nil {
					return false
				}
				return strings.Contains(err.Error(), "panic: test error")
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				c, m := redismock.NewClientMock()
				defer c.Close()

				m.ExpectLIndex("test-queue-processing", 0).SetVal(`{"a":123}`)
				m.ExpectLRem("test-queue-processing", -1, `{"a":123}`).SetVal(1)

				ctrl := newCtrl[Data](c, tc.errOpt)
				ctrl.UnblockProcess()
				if err := ctrl.WaitProcessError(); !tc.checkErr(err) {
					t.Errorf("WaitProcessError() got err = %v, want %v", err, errTest)
				}
				ctrl.WaitStopped()
				if err := m.ExpectationsWereMet(); err != nil {
					t.Error(err)
				}
			})
		}
	})
}
