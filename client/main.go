package main

import (
	"encoding/csv"
	"io"
	"os"
	"strings"

	"github.com/jszwec/csvutil"
	"github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/questions"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
)

//const QPATH = "/files/questions.csv"
//const ADDR = "amqp://guest:guest@tp2_rabbitmq_1:5672/"

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

	queueAddress := v.GetString("rabbitQueue.address")
	//answers := v.GetString("filesPath.answers")
	questionsPath := v.GetString("filesPath.questions")

	conn, err := amqp.Dial(queueAddress)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"input", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

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
	for {
		if i%10000 == 0 {
			log.Println("mensajes enviados: ", i)
		}
		var err error
		j := 0
		for j < chuncksize {
			question := questions.Question{}
			if err = dec.Decode(&question); err == io.EOF {
				break
			} else if err != nil {
				//log.Info(err)
				n_error++
				continue
			}
			//log.Print("lei id ", question.Id)
			//log.Println(" lei UID ", question.OwnerUserId)
			//log.Println(" lei fecha de cierre ", question.ClosedDate)
			tmpQ = append(tmpQ, question)
			i++
			j++
		}
		if err == io.EOF {
			break
		}
		//log.Println("tamaño del chunk ", len(tmpQ))
		err = questions.Publish(ch, q.Name, tmpQ, 1)
		tmpQ = nil
		failOnError(err, "Failed to publish a message")
	}

	tmpQ = append(tmpQ, questions.EndQuestion())
	err = questions.Publish(ch, q.Name, tmpQ, 2)
	failOnError(err, "Failed to publish a message")

	log.Info("mensajes mandados:", i)
	log.Info("errores:", n_error)
}
