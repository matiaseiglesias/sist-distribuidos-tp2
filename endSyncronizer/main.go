package main

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/conn"
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

func loadConfig(v *viper.Viper) []*EndLink {
	links := []*EndLink{}
	params := v.GetStringMap("endOutputs")
	for val, k := range params {
		link_ := &EndLink{}
		v := reflect.ValueOf(k)
		if v.Kind() == reflect.Map {
			for _, key := range v.MapKeys() {
				strct := v.MapIndex(key)

				switch key.Interface().(string) {
				case "out":
					link_.OutQueues = strings.Split(strct.Interface().(string), " ")
				case "n_out":
					link_.NOut = strings.Split(strct.Interface().(string), " ")
				case "n_in":
					link_.NIn = strct.Interface().(int)
				}
			}
		}
		link_.InQueue = val
		link_.Init()
		links = append(links, link_)
	}
	return links
}

func getQueueNames(links []*EndLink) []string {
	qNames := []string{}

	for _, link := range links {
		qNames = append(qNames, link.InQueue)
		qNames = append(qNames, link.OutQueues...)
	}
	return qNames
}

func initConnEndLinks(con *conn.RabbitConn, links []*EndLink) ([]*ConnEndLink, error) {

	var err error
	outConnLinks := []*ConnEndLink{}
	for _, link := range links {

		inMsg, err := con.Input(link.InQueue)
		conn.FailOnError(err, "Failed to register a consumer")
		outConnLinks = append(outConnLinks, &ConnEndLink{
			Link:  link,
			Input: inMsg,
		})

	}
	return outConnLinks, err
}

type EndLink struct {
	NIn          int
	NOut         []string
	OutQueues    []string
	NOutPerQueue map[string]int
	InQueue      string
}

func (e *EndLink) Init() {
	e.NOutPerQueue = make(map[string]int)
	for i, v := range e.OutQueues {
		n, _ := strconv.Atoi(e.NOut[i])
		e.NOutPerQueue[v] = n
	}
}

type ConnEndLink struct {
	Link      *EndLink
	InSignals int
	Input     <-chan amqp.Delivery
}

func main() {

	log.Println("starting inputInterface")
	log.Println("ready to go")

	v, err := InitConfig()
	if err != nil {
		log.Fatalf("%s", err)
	}

	if err := InitLogger(v.GetString("log.level")); err != nil {
		log.Fatalf("%s", err)
	}
	addr := v.GetString("rabbitQueue.address")

	rabbitConn := conn.Init(addr)
	conn_, err := rabbitConn.Connect()
	conn.FailOnError(err, "Failed to connect to RabbitMQ")
	if !conn_ {
		log.Println("error while trying to connect to RabbitMQ, exiting...")
	}

	endLinks := loadConfig(v)
	queueNames := getQueueNames(endLinks)
	endQueueSignal := v.GetString("input.end")
	queueNames = append(queueNames, endQueueSignal)

	rabbitConn.RegisterQueues(queueNames, true)

	endChannel, err := rabbitConn.Input(endQueueSignal)
	conn.FailOnError(err, "Failed to register a consumer")

	endConn, err := initConnEndLinks(rabbitConn, endLinks)
	conn.FailOnError(err, "Failed to init ends connections")

	running := true

	for running {
		for _, e := range endConn {
			select {
			case c := <-e.Input:
				fmt.Println("Me llego una señal del canal: ", e.Link.InQueue)
				e.InSignals++
				fmt.Printf("señales recibidas %d, esperando %d \n", e.InSignals, e.Link.NIn)
				if e.InSignals == e.Link.NIn {
					for endOutput, n := range e.Link.NOutPerQueue {
						fmt.Printf("mando %d señales a %s \n", n, endOutput)
						for i := 0; i < n; i++ {
							rabbitConn.Publish(endOutput, c.Body)
						}
					}
					e.InSignals = 0
				}
				c.Ack(false)
			case e := <-endChannel:
				fmt.Println("end signal received")
				running = false
				e.Ack(false)
			default:
				//fmt.Println("nadie en el canal: ", e.Link.InQueue)
				time.Sleep(500 * time.Millisecond)
			}
		}
	}
	rabbitConn.Close()
	log.Println("Closing endSyncronizer...")
}
