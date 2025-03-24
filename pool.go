package fork_join

import (
	"context"
	"sync"
)

// Pool
// workerCache should be stateless
// cancel is to release goroutine resource
type Pool struct {
	lock         sync.Mutex
	workerCache  sync.Pool
	workers      []*Worker
	cancel       context.CancelFunc
	panicHandler func(interface{})
	err          interface{}
}

func newPool(cancel context.CancelFunc) *Pool {
	p := &Pool{
		cancel: cancel,
	}
	p.panicHandler = func(i interface{}) {
		p.cancel()
		p.err = i
	}
	return p
}

func (p *Pool) Submit(ctx context.Context, jobPayload *JobPayload) {
	w := p.retrieveWorker(ctx)
	w.job <- jobPayload
}

func (p *Pool) retrieveWorker(ctx context.Context) *Worker {
	var w *Worker
	idleWorkers := p.workers

	if len(idleWorkers) > 0 {
		p.lock.Lock()
		n := len(idleWorkers) - 1
		w = idleWorkers[n]
		p.workers = idleWorkers[:n]
		p.lock.Unlock()
	} else {
		if cachedWorker := p.workerCache.Get(); cachedWorker != nil {
			w = cachedWorker.(*Worker)
		} else {
			w = &Worker{
				pool: p,
				job:  make(chan *JobPayload, 1),
			}
		}
		w.run(ctx)
	}
	return w
}

func (p *Pool) releaseWorker(w *Worker) {
	p.lock.Lock()
	p.workers = append(p.workers, w)
	p.lock.Unlock()
}
