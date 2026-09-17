package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
);

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", ContextTimeoutEnabled: true});

	ctx, cancel := context.WithCancel(context.Background());

	sigCh := make(chan os.Signal, 1);
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM);

	queue := NewRedisQueue(rdb, "jobs-pending");

	httpServer := NewHttpServer(":8080",queue);

	go func() {
		fmt.Println("HTTP server listening on :8080");
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("HTTP server error: %v\n", err);
		};
	}();

	go func() {
		<-sigCh                          // 1. OS signal received (Ctrl+C)
		fmt.Println("shutdown requested...")
		cancel()                         // 2. Tell workers/reaper/scheduler to stop (they're watching ctx.Done())
		
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		httpServer.Shutdown(shutdownCtx) // 3. SEPARATELY, explicitly tell the HTTP server to drain and stop
	}();

	var wg sync.WaitGroup = sync.WaitGroup{};
	for i := 0; i < 3; i++ {
		wg.Add(1);
		go redisWorker(ctx, i, queue, &wg);
	};

	go queue.ReapStale(ctx, 15*time.Second) // treat anything unclaimed > 15s as stale

	go queue.runScheduler(ctx);

	// job := NewJob("Id1", "Hey body..", "pending" , "today",0);
	// job2 := NewJob("Id2", "Hey body..", "pending" , "today",1);
	// job3 := NewJob("Id3", "Hey body..", "pending" , "today",2);

	// queue.Enqueue(ctx, job);
	// queue.Enqueue(ctx, job2);
	// queue.Enqueue(ctx, job3);

	wg.Wait();
};