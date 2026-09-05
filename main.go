package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/redis/go-redis/v9"
);

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", ContextTimeoutEnabled: true});

	// err := rdb.Set(redis_ctx, "greeting", "hello redis!", 0).Err();
	// if err != nil {
	// 	panic(err);
	// };

	// val, err := rdb.Get(redis_ctx, "greeting").Result();
	// if err != nil {
	// 	panic(err);
	// };
	// fmt.Println("got back:",val);

	// queue := NewQueue();

	// queue.inFlight.Add(3);

	// go queue.CloseWhenDone();

	ctx, cancel := context.WithCancel(context.Background());

	sigCh := make(chan os.Signal, 1);
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM);

	go func() {
		<-sigCh;
		fmt.Println("shutdown requested...");
		cancel();
	}();

	queue := NewRedisQueue(rdb, "jobs-list");

	var wg sync.WaitGroup = sync.WaitGroup{};
	for i := 0; i < 3; i++ {
		wg.Add(1);
		go redisWorker(ctx, i, queue, &wg);
	};

	job := NewJob("Id1", "Hey body..", "pending" , "today");
	job2 := NewJob("Id2", "Hey body..", "pending" , "today");
	job3 := NewJob("Id3", "Hey body..", "pending" , "today");

	queue.Enqueue(ctx, job);
	queue.Enqueue(ctx, job2);
	queue.Enqueue(ctx, job3);

	wg.Wait();
};