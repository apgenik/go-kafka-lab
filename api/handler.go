package api

import (
	"net/http"

	"github.com/apgenik/go-kafka-lab/kafka"
	"github.com/apgenik/go-kafka-lab/model"

	"github.com/gin-gonic/gin"
)

func CreateMessage(c *gin.Context) {
	var req model.MessageRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.CreateResponse{Status: "error", Error: err.Error()})
		return
	}

	offset, err := kafka.PublishMessage(req.Message)
	if err != nil {
		c.JSON(http.StatusInternalServerError, model.CreateResponse{Status: "error", Error: err.Error()})
		return
	}

	c.JSON(http.StatusOK, model.CreateResponse{Status: "success", Offset: offset})
}

func ReadMessage(c *gin.Context) {
	msg, part, err := kafka.ReadLastMessage()
	if err != nil {
		c.JSON(http.StatusInternalServerError, model.ReadResponse{Error: err.Error()})
		return
	}

	c.JSON(http.StatusOK, model.ReadResponse{Message: msg, Partition: part})
}
