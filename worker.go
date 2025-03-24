package fork_join

import (
	"context"
	"log"
	"time"
)

type Worker struct {
	pool        *Pool
	isRunning   bool
	recycleTime time.Time
	job         chan *JobPayload
}

func (w *Worker) run(ctx context.Context) {
	go func() {
		var tmpTask *ForkJoinTask
		defer func() {
			// handle the error
			// if catch error, should close the channel
			if p := recover(); p != nil {
				w.pool.panicHandler(p)
				if tmpTask != nil {
					w.pool.err = p
					close(tmpTask.result)
				}
			}
		}()

		for {
			select {
			case <-ctx.Done():
				log.Println("an exception occurred, and the task has stopped")
				return
			default:
				for job := range w.job {
					if job == nil {
						// 如果job为空, 则回收到cache中等待之后取用
						w.pool.workerCache.Put(w)
						return
					}
					tmpTask = job.F
					job.F.result <- job.T.Compute()
					w.pool.releaseWorker(w)
				}
			}
		}
	}()
}
