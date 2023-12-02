package main

import (
	"github.com/das08/matlab-jobqueue/connector"
	"os"
	"sync"
)

func main() {
	wg := new(sync.WaitGroup)

	hostname := os.Getenv("MATLAB_QUEUE_HOSTNAME")
	if hostname == "" {
		hostname = "localhost"
	}

	rs := connector.Initialize(hostname)

	// constantly get job
	wg.Add(1)
	go func() {
		for {
			rs.ExecJob()
		}
		wg.Done()
	}()

	wg.Wait()
}
