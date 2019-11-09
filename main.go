package main

import (
	"math/rand"
	"time"

	log "github.com/sirupsen/logrus"
	// "github.com/golang/protobuf/proto"
	// "github.com/stkrizh/otus-go-memcload/appsinstalled"
)

func worker(id int, jobs chan int, results []chan int) {
	for j := range jobs {
		n := 1 + rand.Intn(9)
		log.Debugln("Worker", id, "started  job", j)
		time.Sleep(time.Second * time.Duration(n))
		log.Debugln("Worker", id, "finished job", j)
		results[j] <- j * 2
	}
}

func main() {
	log.SetLevel(log.DebugLevel)
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})

	// working_directory := "/home/sk/Dev/files/"

	nJobs := 5
	jobs := make(chan int)

	results := make([]chan int, nJobs)
	for i := 0; i < nJobs; i++ {
		results[i] = make(chan int)
	}

	log.Debugln("Starting workers...")
	for i := 0; i < nJobs; i++ {
		go worker(i+1, jobs, results)
	}
	log.Debugln("Workers have been started successfully")

	log.Debugln("Creating jobs...")
	for i := 0; i < nJobs; i++ {
		jobs <- i
	}
	close(jobs)
	log.Debugln("Jobs have been created successfully")

	log.Debugln("Waiting for results...")
	for i := 0; i < nJobs; i++ {
		log.Debugf("Got: %d", <-results[i])
	}
}
