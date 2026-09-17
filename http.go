package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
);

type createJobRequest struct {
	Payload string `json:"payload"`
	Priority int    `json:"priority"`
	RunAt    string `json:"runAt,omitempty"`
};

type createJobResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
};

func writeJson(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json");
	w.WriteHeader(status);

	if v != nil {
		json.NewEncoder(w).Encode(v);
	};
};

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJson(w, status, map[string]string{"error":msg});
};

func handleCreateJob(q *RedisQueue) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req createJobRequest;

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body");
			return;
		};

		if req.Payload == "" {
			writeError(w, http.StatusBadRequest, "payload is required");
			return;
		};

		id := fmt.Sprintf("job-%d",time.Now().UnixNano());

		job := NewJob(id, req.Payload, "pending", time.Now().Format(time.RFC3339),req.Priority);

		ctx := r.Context();

		if req.RunAt != "" {
			runAt, err := time.Parse(time.RFC3339, req.RunAt);

			if err != nil {
				writeError(w, http.StatusBadRequest, "runAt must be RFC3339 format, e.g. 2026-09-15T18:00:00Z");
				return;
			};

			if err := q.scheduleJobs(ctx, job, runAt); err != nil {
				writeError(w, http.StatusInternalServerError, "failed to schedule job");
				return;
			};
		} else {
			if err := q.Enqueue(ctx, job); err != nil {
				writeError(w, http.StatusInternalServerError, "failed to enqueue job");
				return;
			};
		};

		writeJson(w, http.StatusCreated, createJobResponse{ID: id, Status:job.Status});
	};
};

func handleGetJob(q *RedisQueue) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id");

		if id == "" {
			writeError(w, http.StatusBadRequest, "job id is required");
			return;
		};

		ctx := r.Context();

		job, err := q.GetJob(ctx, id);

		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to fetch job");
			return;
		};

		if job == nil {
			writeError(w, http.StatusNotFound, "job not found");
			return;
		};

		writeJson(w, http.StatusOK, job);
	};
};

func handleHealth(q *RedisQueue) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context();

		if err := q.client.Ping(ctx).Err(); err != nil {
			writeError(w, http.StatusServiceUnavailable, "redis unreachable");
			return;
		};
		writeJson(w, http.StatusOK, map[string]string{"status": "ok"});
	};
};

func NewHttpServer(addr string, q *RedisQueue) *http.Server {
	mux := http.NewServeMux();

	mux.HandleFunc("POST /jobs", handleCreateJob(q));
	mux.HandleFunc("GET /jobs/{id}", handleGetJob(q));
	mux.HandleFunc("GET /health", handleHealth(q));

	return &http.Server{
		Addr: addr,
		Handler:mux,
	};
};