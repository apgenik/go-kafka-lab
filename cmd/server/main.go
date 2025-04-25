package main

import (
	"github.com/apgenik/go-kafka-lab/api"
	"github.com/gin-gonic/gin"
)

type Event struct {
	Message string `json:"message"`
}

func main() {
	r := gin.Default()

	r.POST("/createmessage", api.CreateMessage)
	r.GET("/readmessage", api.ReadMessage)

	r.Run(":8081")
}
