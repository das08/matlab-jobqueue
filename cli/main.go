package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"math/rand"
	"sync"
	"time"
)

var (
	RedisJobStreamKey = "jobQueueSTR"
	RedisJobGroupKey  = "jobQueueGRP"
)

type redisServer struct {
	rdb *redis.Client
	ctx context.Context
}

func (rs *redisServer) createStreamGroup() {
	// XGROUP CREATE jobQueueSTR jobQueueGRP $ MKSTREAM
	result, err := rs.rdb.XGroupCreateMkStream(rs.ctx, RedisJobStreamKey, RedisJobGroupKey, "$").Result()
	if err != nil {
		if err.Error() != "BUSYGROUP Consumer Group name already exists" {
			panic(err)
		}
	}
	fmt.Println(result)
}

type jobInfo struct {
	id         string
	jobType    string
	hostName   string
	commitHash string
}

func (rs *redisServer) createDummyJob(count int) {
	// XADD jobQueueSTR * jobType build hostName host1 commitHash commit-XXXXX
	for i := 0; i < count; i++ {
		randomId := rand.Intn(10000)
		job := jobInfo{
			jobType:    "build",
			hostName:   "host1",
			commitHash: fmt.Sprintf("commit-%d", randomId),
		}
		_, err := rs.rdb.XAdd(rs.ctx, &redis.XAddArgs{
			Stream: RedisJobStreamKey,
			Values: map[string]interface{}{
				"jobType":    job.jobType,
				"hostName":   job.hostName,
				"commitHash": job.commitHash,
			},
		}).Result()
		if err != nil {
			panic(err)
		}
	}

}

func (rs *redisServer) getJob() {
	// XREADGROUP GROUP jobQueueGRP worker1 COUNT 1 BLOCK 0 STREAMS jobQueueSTR >
	result, err := rs.rdb.XReadGroup(rs.ctx, &redis.XReadGroupArgs{
		Group:    RedisJobGroupKey,
		Consumer: "worker1",
		Streams:  []string{RedisJobStreamKey, ">"},
		Count:    1,
		Block:    0,
	}).Result()
	if err != nil {
		panic(err)
	}
	fmt.Println(result)

	// process job
	if len(result) > 0 {
		job := jobInfo{
			id:         result[0].Messages[0].ID,
			jobType:    result[0].Messages[0].Values["jobType"].(string),
			hostName:   result[0].Messages[0].Values["hostName"].(string),
			commitHash: result[0].Messages[0].Values["commitHash"].(string),
		}
		rs.processJob(job)
	} else {
		fmt.Println("No job")
	}
}

func (rs *redisServer) ackJob(jobId string) {
	// XACK jobQueueSTR jobQueueGRP 1600000000000-0
	_, err := rs.rdb.XAck(rs.ctx, RedisJobStreamKey, RedisJobGroupKey, jobId).Result()

	if err != nil {
		panic(err)
	}
}

func (rs *redisServer) processJob(job jobInfo) {
	// do something
	time.Sleep(10 * time.Second)

	rs.ackJob(job.id)
	fmt.Printf("Finished -> jobType: %s, hostName: %s, commitHash: %s\n", job.jobType, job.hostName, job.commitHash)
}

func main() {
	wg := new(sync.WaitGroup)

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	rs := &redisServer{
		rdb: rdb,
		ctx: context.Background(),
	}
	rs.createStreamGroup()
	rs.createDummyJob(10)

	// constantly get job
	wg.Add(1)
	go func() {
		for {
			rs.getJob()
		}
		wg.Done()
	}()

	wg.Wait()
}
