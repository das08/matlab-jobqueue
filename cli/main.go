package main

import (
	"github.com/das08/matlab-jobqueue/connector"
	"sync"
)

func main() {
	wg := new(sync.WaitGroup)

	rs := connector.Initialize()

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
