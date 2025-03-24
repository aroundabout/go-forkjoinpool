package fork_join

import (
	"context"
	"log"
	"sync"
)

type ForkJoinPool struct {
	Name        string
	cap         int32
	taskQueue   *TaskQueue
	wp          *Pool
	lock        sync.Mutex
	signal      *sync.Cond
	once        sync.Once
	goroutineID int32
	ctx         context.Context
	cancel      context.CancelFunc
	err         interface{}
}

func NewForkJoinPool(name string, cap int32) *ForkJoinPool {
	ctx, cancel := context.WithCancel(context.Background())
	fp := &ForkJoinPool{
		Name:      name,
		cap:       cap,
		taskQueue: NewTaskQueue(cap),
		ctx:       ctx,
		cancel:    cancel,
	}
	fp.wp = newPool(cancel)
	fp.signal = sync.NewCond(new(sync.Mutex))
	fp.run(ctx)
	return fp
}

func (fp *ForkJoinPool) SetPanicHandler(panicHandler func(interface{})) {
	fp.wp.panicHandler = panicHandler
}

func (fp *ForkJoinPool) pushTask(t Task, f *ForkJoinTask) {
	fp.taskQueue.enqueue(t, f)
}

func (fp *ForkJoinPool) run(ctx context.Context) {
	go func() {
		wId := int32(0)
		for {
			select {
			case <-ctx.Done():
				fp.err = fp.wp.err
				log.Println("here is err")
				return
			default:
				hasTask, job, ft := fp.taskQueue.dequeueByTail(wId)
				if hasTask {
					fp.wp.Submit(ctx, &JobPayload{
						T: job,
						F: ft,
						C: ctx,
					})
				}
				wId = (wId + 1) % fp.cap
			}
		}
	}()
}
