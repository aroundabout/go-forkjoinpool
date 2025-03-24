package fork_join

import (
	"math/rand"
	"sync"
)

type TaskQueue struct {
	cap   int32
	nodes [][]*TaskNode
	lock  []sync.Mutex
}

type TaskNode struct {
	task     Task
	consumer *ForkJoinTask
}

func NewTaskQueue(workerCap int32) *TaskQueue {
	taskQueue := &TaskQueue{cap: workerCap}
	taskQueue.nodes = make([][]*TaskNode, workerCap)
	taskQueue.lock = make([]sync.Mutex, workerCap)
	return taskQueue
}

func (taskQueue *TaskQueue) enqueue(e Task, c *ForkJoinTask) {
	id := (c.id - 1) % taskQueue.cap
	defer func() {
		taskQueue.lock[id].Unlock()
	}()
	taskQueue.lock[id].Lock()
	node := &TaskNode{
		task:     e,
		consumer: c,
	}
	taskQueue.nodes[id] = append(taskQueue.nodes[id], node)
}

func (taskQueue *TaskQueue) dequeueByTail(wId int32) (bool, Task, *ForkJoinTask) {
	taskQueue.lock[wId].Lock()
	var element *TaskNode
	if taskQueue.hasNext(wId) {
		n := len(taskQueue.nodes[wId]) - 1
		element = taskQueue.nodes[wId][n]
		taskQueue.nodes[wId] = taskQueue.nodes[wId][:n]
		taskQueue.lock[wId].Unlock()
	} else {
		taskQueue.lock[wId].Unlock()
		cnt := taskQueue.cap
		randomWId := rand.Int31n(cnt)
		for len(taskQueue.nodes[randomWId]) == 0 && cnt != 0 {
			randomWId = rand.Int31n(cnt)
			cnt--
		}
		if cnt == 0 {
			return false, nil, nil
		}
		taskQueue.lock[randomWId].Lock()
		n := len(taskQueue.nodes[randomWId]) - 1
		element = taskQueue.nodes[randomWId][n]
		taskQueue.lock[randomWId].Unlock()
	}
	return true, element.task, element.consumer
}

func (taskQueue *TaskQueue) hasNext(wId int32) bool {
	return len(taskQueue.nodes[wId]) > 0
}

func (taskQueue *TaskQueue) hashKey(v interface{}) int32 {
	return 0
}
