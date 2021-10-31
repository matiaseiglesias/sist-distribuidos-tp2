package main

import (
	"strings"
	"sync"

	"github.com/jszwec/csvutil"
	"github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/answers"
	"github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/questions"
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
	destinos := v.GetStringMapStringSlice("q_destinies")
	for key, element := range destinos {
		log.Println("clave: ", key)
		log.Println("	valor: ", element)
	}
	log.Infof("Log Level: %s", v.GetString("log.level"))
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

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {

		destinos := v.GetStringMapStringSlice("q_destinies")
		queueNames := getKeys(destinos)

		channel := initRabbitQueues(ADDR, queueNames)

		err = channel.Qos(
			1,     // prefetch count
			0,     // prefetch size
			false, // global
		)
		failOnError(err, "Failed to set QoS")

		msgs, err := channel.Consume(
			"input_q", // queue
			"",        // consumer
			false,     // auto-ack
			false,     // exclusive
			false,     // no-local
			false,     // no-wait
			nil,       // args
		)
		failOnError(err, "Failed to register a consumer")

		i := 0
		fallas := 0
		var questions_ []questions.Question
		running := true
		times := 1
		for d := range msgs {
			if err := csvutil.Unmarshal(d.Body, &questions_); err != nil {
				log.Println("error:", err)
				fallas++
				continue
			}
			d.Ack(false)
			for destino, columns := range destinos {
				for _, readQ := range questions_ {

					var filterQ []questions.Question
					if questions.IsEndQuestion(&readQ) {
						log.Println("recibi un mensaje de stop")
						running = false
						filterQ = []questions.Question{readQ}
						times = 1
					} else {
						filterQ = []questions.Question{questions.Filter(readQ, columns)}
					}
					err = questions.Publish(channel, destino, filterQ, times)
					failOnError(err, "Failed to publish a message")

				}
			}
			i += len(questions_)
			questions_ = nil
			if i%5000 == 0 {
				log.Println("[Filtro]mensajes leidos: ", i)
				log.Println("[Filtro]mensajes fallidos: ", fallas)
			}
			if !running {
				log.Println(" bye bye")
				break
			}
		}
		wg.Done()
	}()

	go func() {

		destinos := v.GetStringMapStringSlice("a_destinies")
		queueNames := getKeys(destinos)

		channel := initRabbitQueues(ADDR, queueNames)

		err = channel.Qos(
			1,     // prefetch count
			0,     // prefetch size
			false, // global
		)
		failOnError(err, "Failed to set QoS")

		msgs, err := channel.Consume(
			"input_a", // queue
			"",        // consumer
			false,     // auto-ack
			false,     // exclusive
			false,     // no-local
			false,     // no-wait
			nil,       // args
		)
		failOnError(err, "Failed to register a consumer")

		i := 0
		fallas := 0
		var answers_ []answers.Answer
		running := true
		times := 1
		for d := range msgs {
			if err := csvutil.Unmarshal(d.Body, &answers_); err != nil {
				log.Println("error:", err)
				fallas++
				continue
			}
			d.Ack(false)
			for destino, columns := range destinos {
				for _, readA := range answers_ {

					var filterA []answers.Answer
					if answers.IsEndAnswer(&readA) {
						log.Println("recibi un mensaje de stop")
						running = false
						filterA = []answers.Answer{readA}
						times = 1
					} else {
						filterA = []answers.Answer{answers.Filter(readA, columns)}
					}
					err = answers.Publish(channel, destino, filterA, times)
					failOnError(err, "Failed to publish a message")

				}
			}
			i += len(answers_)
			answers_ = nil
			if i%5000 == 0 {
				log.Println("[Filtro]mensajes leidos: ", i)
				log.Println("[Filtro]mensajes fallidos: ", fallas)
			}
			if !running {
				log.Println(" bye bye")
				break
			}
		}
		wg.Done()
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	wg.Wait()
	log.Printf(" [*] Closing input interface")
}
