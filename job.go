package main
type Job struct {
	ID string `json:"id"`
	Payload string `json:"payload"`
	Status string `json:"status"`
	Priority int `json:"priority"` // 0 = urgent, 1 = high, 2 = normal, 3 = low
	Attempts int `json:"attempts"`
	CreatedAt string `json:"createdAt"`
};

func NewJob(id string, payload string, status string, createdAt string,priority int) *Job {
	return &Job{ID:id, Payload: payload, Status:status, Attempts:0, CreatedAt:createdAt,Priority: priority};
};

