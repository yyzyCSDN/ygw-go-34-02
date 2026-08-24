package queue

import "jobsched/internal/model"

// priorityItems implements heap.Interface ordered by due time.
type priorityItems []model.Task

func (h priorityItems) Len() int { return len(h) }

func (h priorityItems) Less(i, j int) bool {
	if h[i].DueAt.Equal(h[j].DueAt) {
		return h[i].ID < h[j].ID
	}
	return h[i].DueAt.Before(h[j].DueAt)
}

func (h priorityItems) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *priorityItems) Push(x any) {
	*h = append(*h, x.(model.Task))
}

func (h *priorityItems) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
