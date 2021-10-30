package main

import (
	"strings"
	"time"

	"github.com/grassmudhorses/vader-go/lexicon"
	"github.com/grassmudhorses/vader-go/sentitext"
	"github.com/jszwec/csvutil"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
)

const ADDR = "amqp://guest:guest@rabbitmq:5672/"

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
	destinos := v.GetStringMapStringSlice("destinos")
	for key, element := range destinos {
		log.Println("clave: ", key)
		log.Println("     valor: ", element)
	}
	log.Infof("Log Level: %s", v.GetString("log.level"))
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

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

func main() {

	log.Println("starting inputInterface")

	//time.Sleep(70 * time.Second)

	log.Println("ready to go")

	v, err := InitConfig()
	if err != nil {
		log.Fatalf("%s", err)
	}

	if err := InitLogger(v.GetString("log.level")); err != nil {
		log.Fatalf("%s", err)
	}

	// Print program config with debugging purposes
	PrintConfig(v)

	//err = ch.Qos(
	//	1,     // prefetch count
	//	0,     // prefetch size
	//	false, // global
	//)
	//failOnError(err, "Failed to set QoS")

	conn, err := amqp.Dial(ADDR)
	failOnError(err, "Failed to connect to RabbitMQ")
	//defer conn.Close()

	channel, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	//defer ch.Close()

	_, err = channel.QueueDeclare(
		"punto1", // name
		false,    // durable
		false,    // delete when unused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)

	failOnError(err, "Failed to decalre a queue")

	msgs, err := channel.Consume(
		"punto1", // queue
		"",       // consumer
		false,    // auto-ack
		false,    // exclusive
		false,    // no-local
		false,    // no-wait
		nil,      // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		i := 0
		mayor_10 := 0
		fallas := 0
		negativos := 0
		var questions []Question
		for d := range msgs {
			if err := csvutil.Unmarshal(d.Body, &questions); err != nil {
				log.Println("error:", err)
				fallas++
				continue
			}
			if len(questions) == 0 {
				fallas++
				continue
			} else if questions[0].Score > 10 {
				mayor_10++
				parseText := sentitext.Parse(questions[0].Body, lexicon.DefaultLexicon)
				result := sentitext.PolarityScore(parseText)
				if result.Compound < -0.5 {
					negativos++
				}
			}

			if len(questions) > 1 {
				log.Fatal("Se recibio mas de un mensaje!!!")
			}

			d.Ack(false)
			i += len(questions)
			questions = nil
			if i%5000 == 0 {
				log.Println("mensajes leidos: ", i)
				log.Println("mensajes mayores a diez: ", mayor_10)
				log.Println("mensajes fallidos : ", fallas)
				log.Println("mensajes negativos : ", negativos)
			}
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
