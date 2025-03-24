package fork_join

import "context"

type JobPayload struct {
	T Task
	F *ForkJoinTask
	C context.Context
}
