package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"math/rand"
	"time"
)

var (
	JobStreamKey       = "jobQueueSTR"
	JobGroupKey        = "jobQueueGRP"
	CompletedKey       = "completedJobs"
	JobDetailKeyPrefix = "jobDetail"
)

type JobInfo struct {
	Id         string `json:"id"`
	JobType    string `json:"jobType"`
	HostName   string `json:"hostName"`
	CommitHash string `json:"commitHash"`
}

type JobResult struct {
	Id        string `json:"id"`
	JobType   string `json:"jobType"`
	HostName  string `json:"hostName"`
	Timestamp int64  `json:"timestamp"`
	Status    string `json:"status"`
	Message   string `json:"message"`
}

func (jr JobResult) MarshalBinary() ([]byte, error) {
	return json.Marshal(jr)
}

func fromXMessage(xm *redis.XMessage) JobInfo {
	return JobInfo{
		Id:         xm.ID,
		JobType:    xm.Values["jobType"].(string),
		HostName:   xm.Values["hostName"].(string),
		CommitHash: xm.Values["commitHash"].(string),
	}
}

type RedisServer struct {
	rdb        *redis.Client
	ctx        context.Context
	clientHost string
}

func Initialize(hostName string) *RedisServer {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	rs := &RedisServer{
		rdb:        rdb,
		ctx:        context.Background(),
		clientHost: hostName,
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
	// XADD jobQueueSTR * JobType build HostName host1 CommitHash commit-XXXXX
	for i := 0; i < count; i++ {
		randomId := rand.Intn(10000)
		job := JobInfo{
			JobType:    "build",
			HostName:   rs.clientHost,
			CommitHash: fmt.Sprintf("commit-%d", randomId),
		}
		_, err := rs.rdb.XAdd(rs.ctx, &redis.XAddArgs{
			Stream: JobStreamKey,
			Values: map[string]interface{}{
				"jobType":    job.JobType,
				"hostName":   job.HostName,
				"commitHash": job.CommitHash,
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
		job := fromXMessage(&result[0].Messages[0])
		fmt.Printf("Start -> jobID: %s \n", job.Id)

		jobSuccess, jobMessage := rs.processJob(job)

		// TODO: mutex or transaction
		err = rs.ackJob(job.Id)
		if err != nil {
			panic(err)
		}
		err = rs.SetCompletedJob(job.Id, jobSuccess, jobMessage)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Finished -> jobID: %s \n", job.Id)
	} else {
		fmt.Println("No job")
	}
}

func (rs *RedisServer) ackJob(jobId string) error {
	// XACK jobQueueSTR jobQueueGRP 1600000000000-0
	_, err := rs.rdb.XAck(rs.ctx, JobStreamKey, JobGroupKey, jobId).Result()

	return err
}

func (rs *RedisServer) processJob(job JobInfo) (bool, string) {
	// do something
	time.Sleep(5 * time.Second)

	return true, "Job success"
}

func (rs *RedisServer) ReEnqueueAbortedJobs(jobId string) error {
	// XRANGE jobQueueSTR 1600000000000-0 1600000000000-0
	result, err := rs.rdb.XRange(rs.ctx, JobStreamKey, jobId, jobId).Result()
	if err != nil {
		return err
	}

	// XACK jobQueueSTR jobQueueGRP 1600000000000-0
	_, err = rs.rdb.XAck(rs.ctx, JobStreamKey, JobGroupKey, jobId).Result()
	if err != nil {
		return err
	}

	// XDEL jobQueueSTR 1600000000000-0
	_, err = rs.rdb.XDel(rs.ctx, JobStreamKey, jobId).Result()
	if err != nil {
		return err
	}

	// XADD jobQueueSTR * JobType build HostName host1 CommitHash commit-XXXXX
	_, err = rs.rdb.XAdd(rs.ctx, &redis.XAddArgs{
		Stream: JobStreamKey,
		Values: map[string]interface{}{
			"jobType":    result[0].Values["jobType"],
			"hostName":   result[0].Values["hostName"],
			"commitHash": result[0].Values["commitHash"],
		},
	}).Result()
	if err != nil {
		return err
	}

	return nil
}

func (rs *RedisServer) SetCompletedJob(jobId string, success bool, message string) error {
	var status string
	// ZADD completedJobs timestamp jobId
	_, err := rs.rdb.ZAdd(rs.ctx, CompletedKey, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: jobId,
	}).Result()

	if success {
		status = "success"
	} else {
		status = "failed"
	}

	// HSET jobDetail jobId jobDetail
	jobResult := JobResult{
		Id:        jobId,
		JobType:   "***",
		HostName:  rs.clientHost,
		Timestamp: time.Now().Unix(),
		Status:    status,
		Message:   message,
	}

	hsetKey := fmt.Sprintf("%s:%s", JobDetailKeyPrefix, jobId)
	_, err = rs.rdb.HSet(rs.ctx, hsetKey, jobId, jobResult).Result()

	return err
}

func (rs *RedisServer) GetCompletedJobs(count int) ([]JobResult, error) {
	// ZREVRANGE completedJobs 0 count
	result, err := rs.rdb.ZRevRange(rs.ctx, CompletedKey, 0, int64(count)).Result()
	if err != nil {
		return nil, err
	}

	var completedJobs []JobResult
	for _, jobId := range result {
		// HGETALL jobDetail:jobId
		hsetKey := fmt.Sprintf("%s:%s", JobDetailKeyPrefix, jobId)
		jobResult, err := rs.rdb.HGetAll(rs.ctx, hsetKey).Result()
		if err != nil {
			return nil, err
		}

		// convert map to struct
		jr := JobResult{}
		err = json.Unmarshal([]byte(jobResult[jobId]), &jr)
		if err != nil {
			return nil, err
		}
		completedJobs = append(completedJobs, jr)
	}

	return completedJobs, nil
}

func (rs *RedisServer) GetAbortedJobs(count int) ([]JobInfo, error) {
	// XPENDING jobQueueSTR jobQueueGRP - + count
	result, err := rs.rdb.XPendingExt(rs.ctx, &redis.XPendingExtArgs{
		Stream: JobStreamKey,
		Group:  JobGroupKey,
		Start:  "-",
		End:    "+",
		Count:  int64(count),
	}).Result()

	if err != nil {
		return nil, err
	}

	var abortedJobs []JobInfo
	for _, pending := range result {
		// XRANGE jobQueueSTR 1600000000000-0 1600000000000-0 COUNT 1
		xrangeResult, err := rs.rdb.XRange(rs.ctx, JobStreamKey, pending.ID, pending.ID).Result()
		if err != nil {
			return nil, err
		}
		abortedJobs = append(abortedJobs, fromXMessage(&xrangeResult[0]))
	}

	return abortedJobs, nil
}

func (rs *RedisServer) GetPendingJobs(count int) ([]JobInfo, error) {
	// XINFO GROUPS jobQueueSTR
	info, err := rs.rdb.XInfoGroups(rs.ctx, JobStreamKey).Result()
	if err != nil {
		return nil, err
	}
	lastDeliveredId := info[0].LastDeliveredID

	// XREAD STREAMS jobQueueSTR lastDeliveredId
	result, err := rs.rdb.XRead(rs.ctx, &redis.XReadArgs{
		Streams: []string{JobStreamKey, lastDeliveredId},
		Count:   int64(count),
		Block:   0,
	}).Result()

	if err != nil {
		return nil, err
	}

	var pendingJobs []JobInfo
	for _, pending := range result[0].Messages {
		pendingJobs = append(pendingJobs, fromXMessage(&pending))
	}

	return pendingJobs, nil
}
