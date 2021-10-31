package main

import (
	"bytes"
	"encoding/binary"
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

func contains(set []float64, value float64) bool {
	for _, v := range set {
		if v == value {
			return true
		}
	}
	return false
}

func getGroupQueueAddr(m map[string][]string) [][]string {
	var addr [][]string
	for _, val := range m {
		addr = append(addr, val)
	}
	return addr
}

func getQueueAddr(l [][]string) []string {
	var addr []string
	for _, group := range l {
		for _, qName := range group {
			addr = append(addr, qName)
		}
	}
	return addr
}

func getSpecificQueueAddr(m map[string][]string, pos int) []string {
	var addr []string
	for _, group := range m {
		addr = append(addr, group[pos])
	}
	return addr
}

func getNextIndex(current, max int) int {
	current++
	if current == max {
		return 0
	}
	return current
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
	wg.Add(1)

	go func() {

		destinos := v.GetStringMapStringSlice("id_destinies")
		groupQueuesAddr := getGroupQueueAddr(destinos)
		log.Println("grupos disponibles", groupQueuesAddr)
		currentIndex := 0
		maxIndex := len(groupQueuesAddr)
		ids := []float64{}
		idToGroupQueue := make(map[float64][]string)
		var currentQGroupName []string

		qName := v.GetString("q_input")
		aName := v.GetString("a_input")
		qTotalName := v.GetString("total_output")
		addr := v.GetString("rabbitQueue.address")
		channel := initRabbitQueues(addr, append(getQueueAddr(groupQueuesAddr), qTotalName))

		//err = channel.Qos(
		//	1,     // prefetch count
		//	0,     // prefetch size
		//	false, // global
		//)
		//failOnError(err, "Failed to set QoS")

		qMsgs, err := channel.Consume(
			qName, // queue
			"",    // consumer
			false, // auto-ack
			false, // exclusive
			false, // no-local
			false, // no-wait
			nil,   // args
		)
		failOnError(err, "Failed to register a consumer")

		aMsgs, err := channel.Consume(
			aName, // queue
			"",    // consumer
			false, // auto-ack
			false, // exclusive
			false, // no-local
			false, // no-wait
			nil,   // args
		)
		failOnError(err, "Failed to register a consumer")

		qTotales := 0
		var qScore int64
		qScore = 0
		aTotales := 0
		var aScore int64
		aScore = 0

		fallas := 0
		var questions_ []questions.Question
		qRunning := true

		var answers_ []answers.Answer
		aRunning := true

		times := 1

		for {

			if !qRunning && !aRunning {
				log.Println(" bye bye")
				break
			}

			select {
			case d := <-qMsgs:

				if err := csvutil.Unmarshal(d.Body, &questions_); err != nil {
					log.Println("error:", err)
					fallas++
					continue
				}
				d.Ack(false)
				if !contains(ids, questions_[0].OwnerUserId) {
					currentIndex = getNextIndex(currentIndex, maxIndex)
					currentQGroupName = groupQueuesAddr[currentIndex]
					log.Println("agrego el id: ", questions_[0].OwnerUserId)
					log.Println("a la cola: ", currentQGroupName[QQPosition])
					ids = append(ids, questions_[0].OwnerUserId)
					log.Println("ids actualizados: ", ids)
					idToGroupQueue[questions_[0].OwnerUserId] = currentQGroupName
					log.Println("El mapa queda asi: ", idToGroupQueue)
				}

				if questions.IsEndQuestion(&questions_[0]) {
					qRunning = false
					for _, q := range getSpecificQueueAddr(destinos, QQPosition) {
						err = questions.Publish(channel, q, questions_, times)
						failOnError(err, "Failed to publish a message")
					}
				} else {
					log.Println("voy a enviar a la cola:", idToGroupQueue[questions_[0].OwnerUserId][QQPosition])
					err = questions.Publish(channel, idToGroupQueue[questions_[0].OwnerUserId][QQPosition], questions_, times)
					failOnError(err, "Failed to publish a message")
				}
				qScore += questions_[0].Score
				qTotales += len(questions_)

				questions_ = nil
				if qTotales%5000 == 0 {
					log.Println("[idDelivery Q]mensajes leidos: ", qTotales)
					log.Println("[idDelivery Q]mensajes fallidos: ", fallas)
				}

			case a := <-aMsgs:
				if err := csvutil.Unmarshal(a.Body, &answers_); err != nil {
					log.Println("error:", err)
					fallas++
					continue
				}
				a.Ack(false)
				if !contains(ids, answers_[0].OwnerUserId) {
					currentIndex = getNextIndex(currentIndex, maxIndex)
					currentQGroupName = groupQueuesAddr[currentIndex]
					log.Println("agrego el id: ", answers_[0].OwnerUserId)
					log.Println("a la cola: ", currentQGroupName[QQPosition])
					ids = append(ids, answers_[0].OwnerUserId)
					log.Println("ids actualizados: ", ids)
					idToGroupQueue[answers_[0].OwnerUserId] = currentQGroupName
					log.Println("El mapa queda asi: ", idToGroupQueue)
				}

				if answers.IsEndAnswer(&answers_[0]) {
					aRunning = false
					for _, q := range getSpecificQueueAddr(destinos, QAPosition) {
						err = answers.Publish(channel, q, answers_, times)
						failOnError(err, "Failed to publish a message")
					}
				} else {
					log.Println("voy a enviar a la cola:", idToGroupQueue[answers_[0].OwnerUserId][QAPosition])
					err = answers.Publish(channel, idToGroupQueue[answers_[0].OwnerUserId][QAPosition], answers_, times)
					failOnError(err, "Failed to publish a message")
				}
				aScore += answers_[0].Score
				aTotales += len(answers_)

				questions_ = nil
				if aTotales%5000 == 0 {
					log.Println("[idDelivery Q]mensajes leidos: ", aTotales)
					log.Println("[idDelivery Q]mensajes fallidos: ", fallas)
				}
			}
		}

		//first I send questions data

		b := toByteArray(int64(qScore), int64(qTotales))
		err = channel.Publish(
			"",         // exchange
			qTotalName, // routing key
			false,      // mandatory
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         b,
			})
		failOnError(err, "Failed to publish a message")

		// then I send answers data

		b = toByteArray(int64(aScore), int64(aTotales))

		err = channel.Publish(
			"",         // exchange
			qTotalName, // routing key
			false,      // mandatory
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         b,
			})
		failOnError(err, "Failed to publish a message")

		wg.Done()
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	wg.Wait()
	log.Printf(" [*] Closing input interface")
}
