package model

type MessageRequest struct {
	Message string `json:"message" binding:"required"`
}

type CreateResponse struct {
	Status string `json:"status"`
	Offset int64  `json:"offset,omitempty"`
	Error  string `json:"error,omitempty"`
}

type ReadResponse struct {
	Message   string `json:"message"`
	Partition int    `json:"partition"`
	Error     string `json:"error,omitempty"`
}
