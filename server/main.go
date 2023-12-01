package main

import (
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"net/http"
)

func main() {
	e := echo.New()
	e.Use(middleware.Logger())

	e.GET("/", hello)
	e.GET("/jobs", getJobs)

	e.Logger.Fatal(e.Start(":4000"))
}

func hello(c echo.Context) error {
	return c.String(http.StatusOK, "Hello, World!")
}

func getJobs(c echo.Context) error {
	return c.String(http.StatusOK, "Hello, World!")
}
