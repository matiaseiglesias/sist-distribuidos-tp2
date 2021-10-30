package questions

import (
	"log"
	"time"

	"github.com/fatih/structs"
	"github.com/jszwec/csvutil"
	"github.com/mitchellh/mapstructure"
	"github.com/streadway/amqp"
)

const END_ID_NUMBER = -11118

type Question struct {
	Id           int64     `csv:"Id"`
	OwnerUserId  float64   `csv:"OwnerUserId"`
	CreationDate time.Time `csv:"CreationDate"`
	ClosedDate   string    `csv:"ClosedDate"`
	Score        int64     `csv:"Score"`
	Title        string    `csv:"Title"`
	Body         string    `csv:"Body"`
	Tags         string    `csv:"Tags"`
}

func contains(set []string, value string) bool {
	for _, v := range set {
		if v == value {
			return true
		}
	}
	return false
}

func Filter(q Question, columns []string) Question {

	s := structs.Map(q)

	for key := range s {
		if contains(columns, key) {
			continue
		}
		delete(s, key)
	}

	var result Question
	err := mapstructure.Decode(s, &result)
	if err != nil {
		log.Fatal("Esto no funca")
	}
	return result
}

func EndQuestion() Question {
	return Question{
		Id: END_ID_NUMBER,
	}
}

func IsEndQuestion(q *Question) bool {
	return q.Id == END_ID_NUMBER
}

func Publish(ch *amqp.Channel, destiny string, q []Question, times int) error {

	var err error
	data, err := csvutil.Marshal(q)
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
