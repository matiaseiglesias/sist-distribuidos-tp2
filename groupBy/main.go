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

const QSCOREPOSITION = 0
const ASCOREPOSITION = 1

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

func contains(set []float64, value float64) bool {
	for _, v := range set {
		if v == value {
			return true
		}
	}
	return false
}

func toByteArray(id, qScore, aScore float64) []byte {
	buf := new(bytes.Buffer)
	tmp := make([]float64, 3)
	tmp[0] = id
	tmp[1] = qScore
	tmp[2] = aScore
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

		ids := []float64{}
		scoresId := make(map[float64][]int64)

		qName := v.GetString("q_input")
		aName := v.GetString("a_input")
		outQName := v.GetString("output_q")
		outEndSignal := v.GetString("end_signal")

		addr := v.GetString("rabbitQueue.address")
		rabbitConn := conn.Init(addr)
		conn_, err := rabbitConn.Connect()
		conn.FailOnError(err, "Failed to connect to RabbitMQ")
		if !conn_ {
			log.Println("error while trying to connect to RabbitMQ, exiting...")
		}

		queues := []string{qName, aName, outQName, outEndSignal}

		rabbitConn.RegisterQueues(queues, true)

		qMsgs, err := rabbitConn.Input(qName)
		conn.FailOnError(err, "Failed to register a consumer")

		aMsgs, err := rabbitConn.Input(aName)
		conn.FailOnError(err, "Failed to register a consumer")

		total := 0
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
					continue
				}
				if !contains(ids, questions_[0].OwnerUserId) {
					ids = append(ids, questions_[0].OwnerUserId)
					scoresId[questions_[0].OwnerUserId] = []int64{questions_[0].Score, 0}
				} else {
					scores := scoresId[questions_[0].OwnerUserId]
					scores[QSCOREPOSITION] += questions_[0].Score
					scoresId[questions_[0].OwnerUserId] = scores
				}
				total += len(questions_)

				questions_ = nil
				if total%50000 == 0 {
					log.Println("mensajes leidos: ", total)
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
					continue
				}
				if !contains(ids, answers_[0].OwnerUserId) {
					ids = append(ids, answers_[0].OwnerUserId)
					scoresId[answers_[0].OwnerUserId] = []int64{0, answers_[0].Score}
				} else {
					scores := scoresId[answers_[0].OwnerUserId]
					scores[ASCOREPOSITION] += answers_[0].Score
					scoresId[answers_[0].OwnerUserId] = scores
				}
				total += len(answers_)

				answers_ = nil
				if total%50000 == 0 {
					log.Println("mensajes leidos: ", total)
					log.Println("[idDelivery Q]mensajes fallidos: ", failures)
				}
			}
		}

		for id, score := range scoresId {
			b := toByteArray(id, float64(score[0]), float64(score[1]))
			rabbitConn.Publish(outQName, b)
			conn.FailOnError(err, "Failed to publish a message")
		}

		b := toByteArray(-1, -1, -1)
		err = rabbitConn.Publish(outEndSignal, b)
		conn.FailOnError(err, "Failed to publish a message")
		rabbitConn.Close()
		wg.Done()
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	wg.Wait()
	log.Printf(" [*] Closing groupBy")
}
