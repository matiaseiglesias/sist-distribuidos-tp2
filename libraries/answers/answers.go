package answers

import (
	"log"
	"time"

	"github.com/fatih/structs"
	"github.com/jszwec/csvutil"
	"github.com/mitchellh/mapstructure"
	"github.com/streadway/amqp"
)

const END_ID_NUMBER = -11118

type Answer struct {
	Id           int64     `csv:"Id"`
	OwnerUserId  float64   `csv:"OwnerUserId"`
	CreationDate time.Time `csv:"CreationDate"`
	ParentId     int64     `csv:"ParentId"`
	Score        int64     `csv:"Score"`
	Body         string    `csv:"Body"`
}

func contains(set []string, value string) bool {
	for _, v := range set {
		if v == value {
			return true
		}
	}
	return false
}

func Filter(a Answer, columns []string) Answer {

	s := structs.Map(a)

	for key := range s {
		if contains(columns, key) {
			continue
		}
		delete(s, key)
	}

	var result Answer
	err := mapstructure.Decode(s, &result)
	if err != nil {
		log.Fatal("Esto no funca")
	}
	return result
}

func EndAnswer() Answer {
	return Answer{
		Id: END_ID_NUMBER,
	}
}

func IsEndAnswer(q *Answer) bool {
	return q.Id == END_ID_NUMBER
}

func Publish(ch *amqp.Channel, destiny string, a []Answer, times int) error {

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
