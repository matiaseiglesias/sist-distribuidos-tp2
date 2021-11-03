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
		addr := v.GetString("rabbitQueue.address")

		rabbitConn := conn.Init(addr)
		conn_, err := rabbitConn.Connect()
		conn.FailOnError(err, "Failed to connect to RabbitMQ")
		if !conn_ {
			log.Println("error while trying to connect to RabbitMQ, exiting...")
		}

		//err = channel.Qos(
		//	1,     // prefetch count
		//	0,     // prefetch size
		//	false, // global
		//)
		//failOnError(err, "Failed to set QoS")
		qInputName := v.GetString("input.questions")
		endSignalInput := v.GetString("endSignal.input.questions.qname")
		endSignalOutput := v.GetString("endSignal.output.questions.qname")

		queueNames = append(queueNames, qInputName, endSignalInput, endSignalOutput)

		rabbitConn.RegisterQueues(queueNames, true)

		msgs, err := rabbitConn.Input(qInputName)
		conn.FailOnError(err, "Failed to register a consumer")

		i := 0
		fallas := 0
		var questions_ []questions.Question
		running := true
		times := 1
		for d := range msgs {

			if err := csvutil.Unmarshal(d.Body, &questions_); err != nil {
				log.Println("error:", err)
				fallas++
				d.Ack(false)
				continue
			}

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
					err = questions.Publish(rabbitConn.Channel, destino, filterQ, times)
					conn.FailOnError(err, "Failed to publish a message")

				}
			}
			d.Ack(false)
			i += len(questions_)
			questions_ = nil
			if i%100000 == 0 {
				log.Println("[Filter]Read questions: ", i)
				log.Println("[Filter]fail questions: ", fallas)
			}
			if !running {
				log.Println(" bye bye questions")
				break
			}
		}
		rabbitConn.SendEndSync(endSignalOutput, "Filter", 1)

		//log.Println(" waiting question end signal in:", endSignalInput)
		endS, _ := rabbitConn.Input(endSignalInput)
		s := <-endS
		s.Ack(false)
		rabbitConn.Close()
		wg.Done()
	}()

	go func() {

		destinos := v.GetStringMapStringSlice("a_destinies")
		queueNames := getKeys(destinos)
		addr := v.GetString("rabbitQueue.address")

		rabbitConn := conn.Init(addr)
		conn_, err := rabbitConn.Connect()
		conn.FailOnError(err, "Failed to connect to RabbitMQ")
		if !conn_ {
			log.Println("error while trying to connect to RabbitMQ, exiting...")
		}

		qInputName := v.GetString("input.answers")
		endSignalInput := v.GetString("endSignal.input.answers.qname")
		endSignalOutput := v.GetString("endSignal.output.answers.qname")

		queueNames = append(queueNames, qInputName, endSignalInput, endSignalOutput)

		rabbitConn.RegisterQueues(queueNames, true)

		if len(destinos) == 0 {
			log.Println("No answers output")
			rabbitConn.SendEndSync(endSignalOutput, "Filter", 1)
			rabbitConn.Close()
			wg.Done()
			return
		}

		msgs, err := rabbitConn.Input(qInputName)
		conn.FailOnError(err, "Failed to register a consumer")

		i := 0
		fallas := 0
		var answers_ []answers.Answer
		running := true
		times := 1
		for d := range msgs {

			if err := csvutil.Unmarshal(d.Body, &answers_); err != nil {
				log.Println("error:", err)
				fallas++
				d.Ack(false)
				continue
			}
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
					err = answers.Publish(rabbitConn.Channel, destino, filterA, times)
					conn.FailOnError(err, "Failed to publish a message")

				}
			}
			d.Ack(false)
			i += len(answers_)
			answers_ = nil
			if i%100000 == 0 {
				log.Println("[Filter]read answers: ", i)
				log.Println("[Filter]fail answers: ", fallas)
			}
			if !running {
				log.Println(" bye bye answers")
				break
			}
		}
		rabbitConn.SendEndSync(endSignalOutput, "Filter", 1)

		//log.Println(" waiting answer end signal in:", endSignalInput)
		endS, _ := rabbitConn.Input(endSignalInput)
		s := <-endS
		s.Ack(false)
		rabbitConn.Close()
		wg.Done()
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	wg.Wait()
	log.Printf(" [*] Closing input interface")
}
