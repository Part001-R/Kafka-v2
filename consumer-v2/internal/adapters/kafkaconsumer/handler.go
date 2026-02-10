package kafkaconsumer

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
)

type Handler struct{}

func NewHandler() *Handler {

	return &Handler{}
}

func (h *Handler) HandleMessage(message []byte, offset kafka.Offset, consumerNumber int) error {

	logrus.Infof("Rx message: <%s>. Offset: <%d>, consumerNumb: <%d>", string(message), offset, consumerNumber)
	return nil
}
