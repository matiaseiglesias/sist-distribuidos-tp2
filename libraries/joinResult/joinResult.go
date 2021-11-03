package joinResult

import (
	"time"

	"github.com/jszwec/csvutil"
	"github.com/streadway/amqp"
)

const END_ID_NUMBER = -11118

type JoinResult struct {
	Id           int32     `csv:"Id"`
	CreationDate time.Time `csv:"CreationDate"`
	Score        int64     `csv:"Score"`
	Tags         string    `csv:"Tags"`
}

func EndResult() JoinResult {
	return JoinResult{
		Id: END_ID_NUMBER,
	}
}

func IsEndResult(q *JoinResult) bool {
	return q.Id == END_ID_NUMBER
}

func Publish(ch *amqp.Channel, destiny string, a []JoinResult, times int) error {

	var err error
	data, err := csvutil.Marshal(a)
	if err != nil {
		return err
	}

	for i := 0; i < times; i++ {
		err = ch.Publish(
			"",      // exchange
			destiny, // routing key
			false,   // mandatory
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         data,
			})
	}
	return err
}

type GBResult struct {
	Id    int32  `csv:"Id"`
	Year  int    `csv:"CreationDate"`
	Score int64  `csv:"Score"`
	Tag   string `csv:"Tags"`
}

func GBPublish(ch *amqp.Channel, destiny string, a []GBResult, times int) error {

	var err error
	data, err := csvutil.Marshal(a)
	if err != nil {
		return err
	}

	for i := 0; i < times; i++ {
		err = ch.Publish(
			"",      // exchange
			destiny, // routing key
			false,   // mandatory
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         data,
			})
	}
	return err
}

func EndGBResult() GBResult {
	return GBResult{
		Id: END_ID_NUMBER,
	}
}

func IsEndGBResult(q *GBResult) bool {
	return q.Id == END_ID_NUMBER
}
