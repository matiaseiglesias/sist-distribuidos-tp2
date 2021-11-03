package main

import (
	"encoding/csv"
	"io"
	"os"
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

	log.Infof("client configuration")
	log.Infof("rabbit queue address: %s", v.GetString("rabbitQueue.address"))
	log.Infof("questions file path: %s", v.GetString("filesPath.questions"))
	log.Infof("answers file path: %s", v.GetString("filesPath.answers"))
	log.Infof("Log Level: %s", v.GetString("log.level"))
}

func InitLogger(logLevel string) error {
	level, err := log.ParseLevel(logLevel)
	if err != nil {
		return err
	}

	log.SetLevel(level)
	return nil
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

//func syncQRabbit() {
//
//	log.Info("starting client")
//	time.Sleep(70 * time.Second)
//	log.Info("ready to go")
//}

func main() {

	//syncQRabbit()

	v, err := InitConfig()
	if err != nil {
		log.Fatalf("%s", err)
	}

	if err := InitLogger(v.GetString("log.level")); err != nil {
		log.Fatalf("%s", err)
	}

	// Print program config with debugging purposes
	PrintConfig(v)

	addr := v.GetString("rabbitQueue.address")
	questionsPath := v.GetString("filesPath.questions")
	answersPath := v.GetString("filesPath.answers")

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {

		rabbitConn := conn.Init(addr)
		conn_, err := rabbitConn.Connect()
		failOnError(err, "Failed to connect to RabbitMQ")
		if !conn_ {
			log.Println("error while trying to connect to RabbitMQ, exiting...")
		}

		queues := []string{"input_q", "end_2_1_q"}
		rabbitConn.RegisterQueues(queues, true)

		questions_, _ := os.Open(questionsPath)
		r := csv.NewReader(questions_)
		dec, err := csvutil.NewDecoder(r)
		if err != nil {
			log.Fatal(err)
		}
		header := dec.Header()
		log.Println(" header: ", header)

		i := 0
		n_error := 0
		chuncksize := 2
		tmpQ := []questions.Question{}
		var question questions.Question
		for {
			if i%100000 == 0 {
				log.Println("mensajes enviados: ", i)
			}
			var err error
			j := 0
			for j < chuncksize {
				if err = dec.Decode(&question); err == io.EOF {
					break
				} else if err != nil {
					//log.Info(err)
					n_error++
					continue
				}
				tmpQ = append(tmpQ, question)
				i++
				j++
			}

			//log.Println("tamaño del chunk ", len(tmpQ))
			err2 := questions.Publish(rabbitConn.Channel, queues[0], tmpQ, 1)
			tmpQ = nil
			failOnError(err2, "Failed to publish a message")
			if err == io.EOF {
				break
			}
		}

		tmpQ = append(tmpQ, questions.EndQuestion())
		log.Println(" voy a mandar una endQuestion con id ", tmpQ[0].Id)
		err = questions.Publish(rabbitConn.Channel, queues[0], tmpQ, 1)
		failOnError(err, "Failed to publish a message")

		endSignalInput, _ := rabbitConn.Input("end_2_1_q")
		s := <-endSignalInput
		s.Ack(false)

		log.Info("sent questions:", i)
		log.Info("errores:", n_error)
		rabbitConn.Close()
		wg.Done()
	}()

	go func() {

		rabbitConn := conn.Init(addr)
		conn_, err := rabbitConn.Connect()
		failOnError(err, "Failed to connect to RabbitMQ")
		if !conn_ {
			log.Println("error while trying to connect to RabbitMQ, exiting...")
		}

		queues := []string{"input_a", "end_2_1_a"}
		rabbitConn.RegisterQueues(queues, true)

		answers_, _ := os.Open(answersPath)
		r := csv.NewReader(answers_)
		dec, err := csvutil.NewDecoder(r)
		if err != nil {
			log.Fatal(err)
		}
		header := dec.Header()
		log.Println(" header: ", header)

		i := 0
		n_error := 0
		chuncksize := 2
		tmpA := []answers.Answer{}
		for {
			if i%10000 == 0 {
				log.Println("mensajes enviados: ", i)
			}
			var err error
			j := 0
			for j < chuncksize {
				answer := answers.Answer{}
				if err = dec.Decode(&answer); err == io.EOF {
					break
				} else if err != nil {
					//log.Info(err)
					n_error++
					continue
				}
				tmpA = append(tmpA, answer)
				i++
				j++
			}
			//log.Println("tamaño del chunk ", len(tmpQ))
			err2 := answers.Publish(rabbitConn.Channel, queues[0], tmpA, 1)
			tmpA = nil
			failOnError(err2, "Failed to publish a message")
			if err == io.EOF {
				break
			}
		}

		tmpA = append(tmpA, answers.EndAnswer())
		err = answers.Publish(rabbitConn.Channel, queues[0], tmpA, 1)
		failOnError(err, "Failed to publish a message")

		endSignalInput, _ := rabbitConn.Input("end_2_1_a")
		s := <-endSignalInput
		s.Ack(false)

		log.Info("sent answers:", i)
		log.Info("errores:", n_error)
		rabbitConn.Close()
		wg.Done()
	}()

	wg.Wait()
}
