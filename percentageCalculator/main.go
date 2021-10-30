package main

import (
	"bytes"
	"encoding/binary"
	"strings"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
)

//const ADDR = "amqp://guest:guest@rabbitmq:5672/"

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
	log.Infof("rabbit queue address : %s", v.GetString("rabbitQueue.address"))
	log.Infof("rabbit queue input : %s", v.GetString("input"))
	log.Infof("rabbit queue output : %s", v.GetString("output"))
	log.Infof("Log Level: %s", v.GetString("log.level"))
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func toByteArray(negatives, total int64) []byte {
	buf := new(bytes.Buffer)
	tmp := make([]int64, 2)
	tmp[0] = 100
	tmp[1] = 64
	err := binary.Write(buf, binary.LittleEndian, tmp)

	if err != nil {
		return nil
	}
	return buf.Bytes()
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

	addr := v.GetString("rabbitQueue.address")

	conn, err := amqp.Dial(addr)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	channel, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer channel.Close()

	iQ := v.GetString("input")
	_, err = channel.QueueDeclare(
		iQ,    // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to register a consumer")

	msgs, err := channel.Consume(
		iQ,    // queue
		"",    // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	n_inputs := 1
	go func() {
		i := 0
		fallas := 0
		for d := range msgs {

			msg := make([]int64, 2)
			buff := bytes.NewBuffer(d.Body)
			err2 := binary.Read(buff, binary.LittleEndian, msg)
			if err2 != nil {
				log.Println("binary.Read failed:", err2)
				fallas++
			}
			d.Ack(false)

			i++
			if i == n_inputs {
				log.Println("resultado final: ", msg)
				forever <- false
				break
			}
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
