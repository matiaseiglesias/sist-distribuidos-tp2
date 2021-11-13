package main

import (
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
	log.Infof("input questions queue", v.GetString("input.questions"))
	log.Infof("input answers queue", v.GetString("input.answers"))
	log.Infof("input questions end signals ", v.GetString("endSignal.input.questions.qname"))
	log.Infof("output questions end signals rabbit questions output queue", v.GetString("endSignal.output.questions.qname"))
	log.Infof("input answers end signals ", v.GetString("endSignal.input.answers.qname"))
	log.Infof("output answers end signals ", v.GetString("endSignal.output.answers.qname"))
	log.Infof("Log Level: %s", v.GetString("log.level"))
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

func reverse_int(n int64) int64 {
	new_int := int64(0)
	for n > 0 {
		remainder := n % 10
		new_int *= 10
		new_int += remainder
		n /= 10
	}
	return new_int
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

	addr := v.GetString("rabbitQueue.address")
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		rabbitConn := conn.Init(addr)
		conn_, err := rabbitConn.Connect()
		conn.FailOnError(err, "Failed to connect to RabbitMQ")
		if !conn_ {
			log.Println("error while trying to connect to RabbitMQ, exiting...")
		}

		qOutputs := v.GetStringMapStringSlice("q_destinies")
		qQueueNames := getKeys(qOutputs)
		qOutGroups := v.GetStringSlice("q_punto3")
		qInputName := v.GetString("input.questions")
		endQSignal := v.GetString("endSignal.questions")

		queues := []string{}
		queues = append(queues, qQueueNames...)
		queues = append(queues, qOutGroups...)
		queues = append(queues, qInputName, endQSignal)
		rabbitConn.RegisterQueues(queues, true)

		qInput, err := rabbitConn.Input(qInputName)
		conn.FailOnError(err, "Failed to register a consumer")

		qRunning := true
		nQ := 0
		qFailures := 0
		var questions_ []questions.Question

		log.Println("available groups: ", qOutGroups)

		for qRunning {
			q := <-qInput
			if err := csvutil.Unmarshal(q.Body, &questions_); err != nil {
				log.Println("error:", err)
				qFailures++
				q.Ack(false)
				continue
			}
			for _, readQ := range questions_ {

				if questions.IsEndQuestion(&readQ) {
					log.Println("recibi un mensaje de stop")
					qRunning = false
					err = questions.Publish(rabbitConn.Channel, endQSignal, []questions.Question{readQ}, 1)
					conn.FailOnError(err, "Failed to publish a message")
					break
				}
				for destino, columns := range qOutputs {
					var filterQ []questions.Question
					if destino == "q_punto3" {
						//log.Println("grupos disponibles: ", qOutGroups)
						//log.Println("question id: ", readQ.Id)
						//log.Println("mod ", readQ.Id%int64(len(qOutGroups)))
						index := reverse_int(readQ.Id) % int64(len(qOutGroups))
						destino = qOutGroups[index] //delivery.GetGroup(readQ.Id)[0]
					}
					filterQ = []questions.Question{questions.Filter(readQ, columns)}
					err = questions.Publish(rabbitConn.Channel, destino, filterQ, 1)
					conn.FailOnError(err, "Failed to publish a message")
				}
			}
			q.Ack(false)
			nQ += len(questions_)
			questions_ = nil
			if nQ%50000 == 0 {
				log.Println("[Filter]Read questions: ", nQ)
				log.Println("[Filter]fail answers: ", qFailures)
			}
		}
		rabbitConn.Close()
		wg.Done()
	}()

	go func() {
		rabbitConn := conn.Init(addr)
		conn_, err := rabbitConn.Connect()
		conn.FailOnError(err, "Failed to connect to RabbitMQ")
		if !conn_ {
			log.Println("error while trying to connect to RabbitMQ, exiting...")
		}

		aOutputs := v.GetStringMapStringSlice("a_destinies")
		aQueueNames := getKeys(aOutputs)
		aOutGroups := v.GetStringSlice("a_punto3")

		aInputName := v.GetString("input.answers")
		endASignal := v.GetString("endSignal.answers")

		queues := []string{}
		queues = append(queues, aQueueNames...)
		queues = append(queues, aOutGroups...)
		queues = append(queues, aInputName, endASignal)

		rabbitConn.RegisterQueues(queues, true)

		aInput, err := rabbitConn.Input(aInputName)
		conn.FailOnError(err, "Failed to register a consumer")

		aRunning := true
		nA := 0
		aFailures := 0
		var answers_ []answers.Answer
		log.Println("available groups: ", aOutGroups)
		for aRunning {
			a := <-aInput
			if err := csvutil.Unmarshal(a.Body, &answers_); err != nil {
				log.Println("error:", err)
				aFailures++
				a.Ack(false)
				continue
			}
			for _, readA := range answers_ {
				if answers.IsEndAnswer(&readA) {
					log.Println("recibi un mensaje de stop")
					aRunning = false
					err = answers.Publish(rabbitConn.Channel, endASignal, []answers.Answer{readA}, 1)
					conn.FailOnError(err, "Failed to publish a message")
					break
				}
				for destino, columns := range aOutputs {
					newDestino := destino
					var filterA []answers.Answer
					if destino == "a_punto3" {
						//log.Println("grupos disponibles: ", aOutGroups)
						//log.Println("question id: ", readA.ParentId)
						//log.Println("mod ", readA.ParentId%int64(len(aOutGroups)))
						index := reverse_int(readA.ParentId) % int64(len(aOutGroups))
						newDestino = aOutGroups[index] //delivery.GetGroup(readA.ParentId)[0]
						//log.Println("destino: ", newDestino)
					}
					filterA = []answers.Answer{answers.Filter(readA, columns)}
					err = answers.Publish(rabbitConn.Channel, newDestino, filterA, 1)
					conn.FailOnError(err, "Failed to publish a message")
				}
			}
			a.Ack(false)
			nA += len(answers_)
			answers_ = nil
			if nA%50000 == 0 {
				log.Println("[Filter]Read answers: ", nA)
				log.Println("[Filter]fail questions: ", aFailures)
			}
		}
		rabbitConn.Close()
		wg.Done()
	}()

	wg.Wait()
	log.Printf(" [*] Closing input interface")
}
