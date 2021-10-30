package main

import (
	"strings"
	"time"

	"github.com/fatih/structs"
	"github.com/jszwec/csvutil"
	"github.com/mitchellh/mapstructure"
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
		log.Println("	valor: ", element)
	}
	log.Infof("Log Level: %s", v.GetString("log.level"))
}

func contains(set []string, value string) bool {
	for _, v := range set {
		if v == value {
			return true
		}
	}
	return false
}

func filter(q Question, columns []string) Question {

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

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func initRabbitQueues(addr string, names []string) *amqp.Channel {

	conn, err := amqp.Dial(addr)
	failOnError(err, "Failed to connect to RabbitMQ")
	//defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	//defer ch.Close()

	_, err = ch.QueueDeclare(
		"input", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to open a channel")

	for _, name := range names {
		_, err := ch.QueueDeclare(
			name,  // name
			false, // durable
			false, // delete when unused
			false, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		failOnError(err, "Failed to open a channel")

	}
	return ch
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

func getKeys(myMap map[string][]string) []string {
	keys := make([]string, len(myMap))
	i := 0
	for k := range myMap {
		keys[i] = k
		i++
	}
	return keys
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

	destinos := v.GetStringMapStringSlice("destinos")
	queueNames := getKeys(destinos)

	channel := initRabbitQueues(ADDR, queueNames)

	//err = ch.Qos(
	//	1,     // prefetch count
	//	0,     // prefetch size
	//	false, // global
	//)
	//failOnError(err, "Failed to set QoS")

	msgs, err := channel.Consume(
		"input", // queue
		"",      // consumer
		false,   // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		i := 0
		fallas := 0
		var questions []Question
		for d := range msgs {
			if err := csvutil.Unmarshal(d.Body, &questions); err != nil {
				log.Println("error:", err)
				fallas++
				continue
			}
			for destino, columns := range destinos {
				for _, readQ := range questions {
					filterQ := []Question{filter(readQ, columns)}

					b, err := csvutil.Marshal(filterQ)
					if err != nil {
						log.Println("error:", err)
					}

					err = channel.Publish(
						"",      // exchange
						destino, // routing key
						false,   // mandatory
						false,
						amqp.Publishing{
							DeliveryMode: amqp.Persistent,
							ContentType:  "text/plain",
							Body:         b,
						})
					failOnError(err, "Failed to publish a message")
				}
			}
			d.Ack(false)
			i += len(questions)
			questions = nil
			if i%5000 == 0 {
				log.Println("[Filtro]mensajes leidos: ", i)
				log.Println("[Filtro]mensajes fallidos: ", fallas)
			}
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

}
