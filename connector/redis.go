package connector

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"math/rand"
	"time"
)

var (
	JobStreamKey = "jobQueueSTR"
	JobGroupKey  = "jobQueueGRP"
	CompletedKey = "completedJobs"
)

type JobInfo struct {
	id         string
	jobType    string
	hostName   string
	commitHash string
}

type RedisServer struct {
	rdb *redis.Client
	ctx context.Context
}

func Initialize() *RedisServer {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	rs := &RedisServer{
		rdb: rdb,
		ctx: context.Background(),
	}

	rs.createStreamGroup()

	return rs
}

func (rs *RedisServer) createStreamGroup() {
	// XGROUP CREATE jobQueueSTR jobQueueGRP $ MKSTREAM
	result, err := rs.rdb.XGroupCreateMkStream(rs.ctx, JobStreamKey, JobGroupKey, "$").Result()
	if err != nil {
		if err.Error() != "BUSYGROUP Consumer Group name already exists" {
			panic(err)
		}
	}
	fmt.Println(result)
}

func (rs *RedisServer) CreateDummyJob(count int) {
	// XADD jobQueueSTR * jobType build hostName host1 commitHash commit-XXXXX
	for i := 0; i < count; i++ {
		randomId := rand.Intn(10000)
		job := JobInfo{
			jobType:    "build",
			hostName:   "host1",
			commitHash: fmt.Sprintf("commit-%d", randomId),
		}
		_, err := rs.rdb.XAdd(rs.ctx, &redis.XAddArgs{
			Stream: JobStreamKey,
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

func (rs *RedisServer) ExecJob() {
	// XREADGROUP GROUP jobQueueGRP worker1 COUNT 1 BLOCK 0 STREAMS jobQueueSTR >
	result, err := rs.rdb.XReadGroup(rs.ctx, &redis.XReadGroupArgs{
		Group:    JobGroupKey,
		Consumer: "worker1",
		Streams:  []string{JobStreamKey, ">"},
		Count:    1,
		Block:    0,
	}).Result()
	if err != nil {
		panic(err)
	}

	// process job
	if len(result) > 0 {
		job := JobInfo{
			id:         result[0].Messages[0].ID,
			jobType:    result[0].Messages[0].Values["jobType"].(string),
			hostName:   result[0].Messages[0].Values["hostName"].(string),
			commitHash: result[0].Messages[0].Values["commitHash"].(string),
		}
		fmt.Printf("Start -> jobID: %s \n", job.id)
		err := rs.processJob(job)
		if err != nil {
			panic(err)
		}
	} else {
		fmt.Println("No job")
	}
}

func (rs *RedisServer) ackJob(jobId string) error {
	// XACK jobQueueSTR jobQueueGRP 1600000000000-0
	_, err := rs.rdb.XAck(rs.ctx, JobStreamKey, JobGroupKey, jobId).Result()

	return err
}

func (rs *RedisServer) processJob(job JobInfo) error {
	// do something
	time.Sleep(10 * time.Second)

	err := rs.ackJob(job.id)
	if err != nil {
		return err
	}
	err = rs.SetCompletedJob(job.id)
	if err != nil {
		return err
	}
	fmt.Printf("Finished -> jobID: %s \n", job.id)

	return nil
}

func (rs *RedisServer) SetCompletedJob(jobId string) error {
	// ZADD completedJobs timestamp jobId
	_, err := rs.rdb.ZAdd(rs.ctx, CompletedKey, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: jobId,
	}).Result()

	return err
}
