package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
);

func main() {
	queue := NewQueue();

	queue.inFlight.Add(3);

	go queue.CloseWhenDone();

	ctx, cancel := context.WithCancel(context.Background());

	sigCh := make(chan os.Signal, 1);
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM);

	go func() {
		<-sigCh;
		fmt.Println("shutdown requested...");
		cancel();
	}();

	var wg sync.WaitGroup = sync.WaitGroup{};
	for i := 0; i < 3; i++ {
		wg.Add(1);
		go worker(ctx,i,queue,&wg);
	};

	job := NewJob("Id1", "Hey body..", "pending" , "today");
	job2 := NewJob("Id2", "Hey body..", "pending" , "today");
	job3 := NewJob("Id3", "Hey body..", "pending" , "today");

	queue.Enqueue(job);
	queue.Enqueue(job2);
	queue.Enqueue(job3);

	wg.Wait();
};