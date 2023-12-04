package main

import (
	"github.com/das08/matlab-jobqueue/connector"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"net/http"
	"strconv"
)

var (
	rs *connector.RedisServer
)

func main() {
	e := echo.New()
	e.Use(middleware.Logger())

	e.GET("/", hello)
	e.GET("/jobs/completed", getCompletedJobs)
	e.GET("/jobs/aborted", getAbortedJobs)
	e.GET("/jobs/pending", getPendingJobs)
	e.POST("/debug/create", createDummyJob)
	e.POST("/jobs/reenqueue/:id", reenqueueJob)

	// initialize redis server
	rs = connector.Initialize("webserver.local")

	e.Logger.Fatal(e.Start(":4000"))
}

func hello(c echo.Context) error {
	return c.String(http.StatusOK, "Hello, World!")
}

func createDummyJob(c echo.Context) error {
	rs.CreateDummyJob(2)
	return c.JSON(http.StatusOK, "OK")
}

func getCompletedJobs(c echo.Context) error {
	count := c.QueryParam("count")
	if count == "" {
		count = "10"
	}
	countInt, _ := strconv.Atoi(count)
	jobs, err := rs.GetCompletedJobs(countInt)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, jobs)
}

func getAbortedJobs(c echo.Context) error {
	count := c.QueryParam("count")
	if count == "" {
		count = "10"
	}
	countInt, _ := strconv.Atoi(count)
	jobs, err := rs.GetAbortedJobs(countInt)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, jobs)
}

func getPendingJobs(c echo.Context) error {
	count := c.QueryParam("count")
	if count == "" {
		count = "10"
	}
	countInt, _ := strconv.Atoi(count)
	jobs, err := rs.GetPendingJobs(countInt)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, jobs)
}

func reenqueueJob(c echo.Context) error {
	id := c.Param("id")
	err := rs.ReEnqueueAbortedJobs(id)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, "OK")
}
