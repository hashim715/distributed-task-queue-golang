package main

type Job struct {
	ID string
	Payload string
	Status string
	Attempts int
	CreatedAt string
};

func NewJob(id string, payload string, status string, createdAt string) *Job {
	return &Job{ID:id, Payload: payload, Status:status, Attempts:0, CreatedAt:createdAt};
};

