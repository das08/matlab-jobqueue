package main

import (
	"github.com/das08/matlab-jobqueue/connector"
	"sync"
)

func main() {
	wg := new(sync.WaitGroup)

	//rdb := redis.NewClient(&redis.Options{
	//	Addr:     "localhost:6379",
	//	Password: "",
	//	DB:       0,
	//})

	rs := connector.Initialize()

	// constantly get job
	wg.Add(1)
	go func() {
		for {
			rs.GetJob()
		}
		wg.Done()
	}()

	wg.Wait()
}
