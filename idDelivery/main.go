package main

import (
	"bytes"
	"encoding/binary"
	"strings"
	"sync"

	"github.com/jszwec/csvutil"
	"github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/answers"
	"github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/conn"
	"github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/questions"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const QAPosition = 0
const QQPosition = 1

func InitLogger(logLevel string) error {
	level, err := log.ParseLevel(logLevel)
	if err != nil {
		return err
	}

	log.SetLevel(level)
	return nil
}

func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	v.AutomaticEnv()
	v.SetEnvPrefix("cli")
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Try to read configuration from config file. If config file
	// does not exists then ReadInConfig will fail but configuration
	// can be loaded from the environment variables so we shouldn't
	// return an error in that case
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		log.Info("Configuration could not be read from config file.")
		errors.Wrapf(err, "Configuration could not be read from config file.")
	}

	return v, nil
}

func PrintConfig(v *viper.Viper) {

	log.Infof("Filter configuration")
	log.Infof("rabbit queue address: %s", v.GetString("rabbitQueue.address"))
	destinos := v.GetStringMapStringSlice("q_destinies")
	for key, element := range destinos {
		log.Println("clave: ", key)
		log.Println("	valor: ", element)
	}
	log.Infof("Log Level: %s", v.GetString("log.level"))
}

func toByteArray(negatives, total int64) []byte {
	buf := new(bytes.Buffer)
	tmp := make([]int64, 2)
	tmp[0] = negatives
	tmp[1] = total
	err := binary.Write(buf, binary.LittleEndian, tmp)

	if err != nil {
		return nil
	}
	return buf.Bytes()
}

func main() {

	log.Println("starting inputInterface")

	v, err := InitConfig()
	if err != nil {
		log.Fatalf("%s", err)
	}

	if err := InitLogger(v.GetString("log.level")); err != nil {
		log.Fatalf("%s", err)
	}

	// Print program config with debugging purposes
	PrintConfig(v)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {

		destinos := v.GetStringMapStringSlice("id_destinies")
		del := &conn.Delivery{}
		del.Init(destinos)

		addr := v.GetString("rabbitQueue.address")
		rabbitConn := conn.Init(addr)
		conn_, err := rabbitConn.Connect()
		conn.FailOnError(err, "Failed to connect to RabbitMQ")
		if !conn_ {
			log.Println("error while trying to connect to RabbitMQ, exiting...")
		}

		qName := v.GetString("q_input")
		aName := v.GetString("a_input")
		qTotalName := v.GetString("total_output")
		qEndSignal := v.GetString("endSignal.questions")
		aEndSignal := v.GetString("endSignal.answers")
		//tEndSignal := v.GetString("endSignal.total")

		queues := []string{qName, aName, qTotalName}
		queues = append(queues, del.GetOutQueueNames()...)
		log.Println("colas a registrar: ", queues)
		rabbitConn.RegisterQueues(queues, true)

		qMsgs, err := rabbitConn.Input(qName)
		conn.FailOnError(err, "Failed to register a consumer")

		aMsgs, err := rabbitConn.Input(aName)
		conn.FailOnError(err, "Failed to register a consumer")

		qTotales := 0
		var qScore int64
		qScore = 0
		aTotales := 0
		var aScore int64
		aScore = 0

		failures := 0
		var questions_ []questions.Question
		qRunning := true

		var answers_ []answers.Answer
		aRunning := true

		for qRunning || aRunning {

			select {
			case d := <-qMsgs:

				if err := csvutil.Unmarshal(d.Body, &questions_); err != nil {
					log.Println("error:", err)
					failures++
					continue
				}
				d.Ack(false)

				if questions.IsEndQuestion(&questions_[0]) {
					qRunning = false
					err = questions.Publish(rabbitConn.Channel, qEndSignal, questions_, 1)
					conn.FailOnError(err, "Failed to pufalseblish a message")
					continue
				} else {
					qQueueName := del.GetGroup(questions_[0].OwnerUserId)[QQPosition]
					//log.Println("output question queue:", qQueueName)
					err = questions.Publish(rabbitConn.Channel, qQueueName, questions_, 1)
					conn.FailOnError(err, "Failed to publish a message")
				}
				qScore += questions_[0].Score
				qTotales += len(questions_)

				questions_ = nil
				if qTotales%50000 == 0 {
					log.Println("[idDelivery Q]mensajes leidos: ", qTotales)
					log.Println("[idDelivery Q]mensajes fallidos: ", failures)
				}

			case a := <-aMsgs:
				if err := csvutil.Unmarshal(a.Body, &answers_); err != nil {
					log.Println("error:", err)
					failures++
					continue
				}
				a.Ack(false)
				if answers.IsEndAnswer(&answers_[0]) {
					aRunning = false
					err = answers.Publish(rabbitConn.Channel, aEndSignal, answers_, 1)
					conn.FailOnError(err, "Failed to publish a message")
					continue

				} else {
					aQueueName := del.GetGroup(answers_[0].OwnerUserId)[QAPosition]
					//log.Println("output question queue:", aQueueName)
					err = answers.Publish(rabbitConn.Channel, aQueueName, answers_, 1)
					conn.FailOnError(err, "Failed to publish a message")
				}
				aScore += answers_[0].Score
				aTotales += len(answers_)

				questions_ = nil
				if aTotales%50000 == 0 {
					log.Println("[idDelivery Q]mensajes leidos: ", aTotales)
					log.Println("[idDelivery Q]mensajes fallidos: ", failures)
				}
			}
		}
		//first I send questions data
		log.Println("[idDelivery Q]results qscore: ", qScore)
		log.Println("[idDelivery Q]results total q: ", qTotales)
		b := toByteArray(int64(qScore), int64(qTotales))
		err = rabbitConn.Publish(qTotalName, b)
		conn.FailOnError(err, "Failed to publish a message")

		// then I send answers data
		log.Println("[idDelivery Q]results qscore: ", aScore)
		log.Println("[idDelivery Q]results total q: ", aTotales)
		b = toByteArray(int64(aScore), int64(aTotales))
		err = rabbitConn.Publish(qTotalName, b)
		conn.FailOnError(err, "Failed to publish a message")

		wg.Done()
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	wg.Wait()
	log.Printf(" [*] Closing input interface")
}
