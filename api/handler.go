package api

import (
	"context"
	"net/http"
	"time"

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

//
//

type Server struct {
	router   *gin.Engine
	producer *kafka.Producer
	consumer *kafka.Consumer
}

// возвращает сервер
func NewServer(producer *kafka.Producer, consumer *kafka.Consumer) *Server {
	s := &Server{
		router:   gin.Default(),
		producer: producer,
		consumer: consumer,
	}
	s.setupRoutes()
	return s
}

func (s *Server) setupRoutes() {
	s.router.POST("/createmessage", s.handleCreateMessage)
	s.router.GET("/readmessage", s.handleReadMessage)
	s.router.GET("/ping", s.ping)
}

func (s *Server) Start(port string) error {
	return s.router.Run(":" + port)
}

func (s *Server) Stop() {
	// Graceful shutdown можно добавить
}

func (s *Server) ping(c *gin.Context) {
	c.JSON(200, gin.H{
		"message": "pong",
	})
}

func (s *Server) handleCreateMessage(c *gin.Context) {
	var req struct {
		Message string `json:"message" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	_, err := s.producer.Publish(req.Message)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		// Offset временно недоступен в этой реализации
	})
}

func (s *Server) handleReadMessage(c *gin.Context) {
	msg, err := s.consumer.ReadMessage()
	if err != nil {
		if err == context.DeadlineExceeded {
			c.JSON(http.StatusNotFound, gin.H{"error": "no messages available"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":   msg,
		"timestamp": time.Now().UTC(),
	})
}
