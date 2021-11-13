package main

import (
	"strings"
	"sync"

	"github.com/jszwec/csvutil"
	"github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/answers"
	"github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/conn"
	"github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/joinResult"
	"github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/questions"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
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
	log.Infof("input questions queue: %s", v.GetString("q_input"))
	log.Infof("input answers queue: %s", v.GetString("a_input"))
	log.Infof("output queue: %s", v.GetString("join_output"))
	log.Infof("Log Level: %s", v.GetString("log.level"))
}

func getKeys(myMap map[int64][]*joinResult.JoinResult) []int64 {
	keys := make([]int64, len(myMap))
	i := 0
	for k := range myMap {
		keys[i] = k
		i++
	}
	return keys
}

func contains(set []int64, value int64) bool {
	for _, v := range set {
		if v == value {
			return true
		}
	}
	return false
}

func addTags(r []*joinResult.JoinResult, tags string) {
	for _, result := range r {
		result.Tags = tags
	}
}

func publishAllResults(ch *amqp.Channel, d []string, r []*joinResult.JoinResult) {
	for _, result := range r {
		qOutName := d[result.CreationDate.Year()%len(d)]
		joinResult.Publish(ch, qOutName, []joinResult.JoinResult{*result}, 1)
	}
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

		addr := v.GetString("rabbitQueue.address")
		qName := v.GetString("q_input")
		aName := v.GetString("a_input")
		OutGroups := v.GetStringSlice("destinies")
		endSignalOutput := v.GetString("endSignal")

		rabbitConn := conn.Init(addr)
		conn_, err := rabbitConn.Connect()
		conn.FailOnError(err, "Failed to connect to RabbitMQ")
		if !conn_ {
			log.Println("error while trying to connect to RabbitMQ, exiting...")
		}
		queuesName := []string{qName, aName, endSignalOutput}
		queuesName = append(queuesName, OutGroups...)
		rabbitConn.RegisterQueues(queuesName, true)

		qMsgs, err := rabbitConn.Input(qName)
		conn.FailOnError(err, "Failed to register a consumer")

		aMsgs, err := rabbitConn.Input(aName)
		conn.FailOnError(err, "Failed to register a consumer")

		i := 0
		failures := 0
		var questions_ []questions.Question
		var answers_ []answers.Answer
		aRunning := true
		qRunning := true
		times := 1
		qIdToResult := make(map[int64]*joinResult.JoinResult)
		qIds := make([]int64, 0)
		aResultStorage := make(map[int64][]*joinResult.JoinResult)

		for aRunning || qRunning {

			select {
			case q := <-qMsgs:
				if err := csvutil.Unmarshal(q.Body, &questions_); err != nil {
					log.Println("error:", err)
					failures++
					q.Ack(false)
					continue
				}

				if questions.IsEndQuestion(&questions_[0]) {
					qRunning = false
					q.Ack(false)
					continue
				}
				qIds = append(qIds, questions_[0].Id)
				qIdToResult[questions_[0].Id] = &joinResult.JoinResult{
					CreationDate: questions_[0].CreationDate,
					Score:        questions_[0].Score,
					Tags:         questions_[0].Tags,
				}
				//log.Println("agregue el result: ", qIdToResult[questions_[0].Id])
				aIds := getKeys(aResultStorage)
				if contains(aIds, questions_[0].Id) {
					//log.Println("hay match con parent id guardado= :", questions_[0].Id)
					aResults := aResultStorage[questions_[0].Id]
					tags := qIdToResult[questions_[0].Id].Tags
					addTags(aResults, tags)

					publishAllResults(rabbitConn.Channel, OutGroups, aResults)
					delete(aResultStorage, questions_[0].Id)
				}
				q.Ack(false)
				questions_ = nil

			case a := <-aMsgs:

				if err := csvutil.Unmarshal(a.Body, &answers_); err != nil {
					log.Println("error:", err)
					failures++
					a.Ack(false)
					continue
				}
				if answers.IsEndAnswer(&answers_[0]) {
					aRunning = false
					a.Ack(false)
					continue
				}
				//log.Println("recibi una respuesta con parent id= :", answers_[0].ParentId)
				j := &joinResult.JoinResult{

					CreationDate: answers_[0].CreationDate,
					Score:        answers_[0].Score,
				}
				if contains(qIds, answers_[0].ParentId) {
					//log.Println("hay match con parent id= :", answers_[0].ParentId)
					tags := qIdToResult[answers_[0].ParentId].Tags
					j.Tags = tags

					qOutName := OutGroups[j.CreationDate.Year()%len(OutGroups)]
					joinResult.Publish(rabbitConn.Channel, qOutName, []joinResult.JoinResult{*j}, times)
				} else {
					//log.Println("chequeo parent id= :", answers_[0].ParentId)
					aResultStorage[answers_[0].ParentId] = append(aResultStorage[answers_[0].ParentId], j)
				}
				a.Ack(false)
				answers_ = nil
			}
			i++
			if i%50000 == 0 {
				log.Println("mensajes procesados: ", i)
			}
		}

		log.Println("lose answers: ", len(aResultStorage))
		log.Println("stored questions n: ", len(qIdToResult))
		for _, qResult := range qIdToResult {
			qOutName := OutGroups[qResult.CreationDate.Year()%len(OutGroups)]
			err = joinResult.Publish(rabbitConn.Channel, qOutName, []joinResult.JoinResult{*qResult}, times)
			conn.FailOnError(err, "fail while publishing")
		}
		endR := []joinResult.JoinResult{joinResult.EndResult()}
		joinResult.Publish(rabbitConn.Channel, endSignalOutput, endR, times)

		rabbitConn.Close()
		wg.Done()
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	wg.Wait()
	log.Printf(" [*] Closing input interface")
}
