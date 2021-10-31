package main

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"strings"

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
	log.Infof("rabbit queue address : %s", v.GetString("rabbitQueue.address"))
	log.Infof("rabbit queue input from idDelivery : %s", v.GetString("total_input.addr"))
	log.Infof("rabbit queue output : %s", v.GetString("output"))
	log.Infof("Log Level: %s", v.GetString("log.level"))
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// An Item is something we manage in a priority queue.
type Item struct {
	id       float64 // The value of the item; arbitrary.
	priority float64 // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].priority > pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *Item, value float64, priority float64) {
	item.id = value
	item.priority = priority
	heap.Fix(pq, item.index)
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

	scoresQ := v.GetString("scores_input")
	_, err = channel.QueueDeclare(
		scoresQ, // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to register a consumer")

	scoresMsgs, err := channel.Consume(
		scoresQ, // queue
		"",      // consumer
		false,   // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)
	failOnError(err, "Failed to register a consumer")

	totalScoresQ := v.GetString("total_input.addr")
	_, err = channel.QueueDeclare(
		totalScoresQ, // name
		false,        // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to register a consumer")

	totalScoresMsgs, err := channel.Consume(
		totalScoresQ, // queue
		"",           // consumer
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	failOnError(err, "Failed to register a consumer")

	nTotalScores := 2 // v.GetInt("total_input.n")
	nMsg := 0

	scoresAverage := make(map[string]int64)

	for nMsg < nTotalScores {
		tScores := <-totalScoresMsgs
		msg := make([]int64, 2)
		buff := bytes.NewBuffer(tScores.Body)
		err2 := binary.Read(buff, binary.LittleEndian, msg)
		if err2 != nil {
			log.Println("binary.Read failed:", err2)
			nMsg++
			continue
		}
		tScores.Ack(false)
		if nMsg == 0 {
			log.Println("questions score:", msg[0])
			log.Println("total questions:", msg[1])
			scoresAverage["questions"] = msg[0] / msg[1]
		} else if nMsg == 1 {
			log.Println("answers score:", msg[0])
			log.Println("total answers:", msg[1])
			scoresAverage["answers"] = msg[0] / msg[1]
		}
		nMsg++
	}

	pq := make(PriorityQueue, 0)
	heap.Init(&pq)

	i := 0
	fallas := 0
	for d := range scoresMsgs {

		msg := make([]float64, 3)
		buff := bytes.NewBuffer(d.Body)
		err2 := binary.Read(buff, binary.LittleEndian, msg)
		if err2 != nil {
			log.Println("binary.Read failed:", err2)
			fallas++
			d.Ack(false)
			continue
		}
		d.Ack(false)

		if msg[0] == -1 && msg[1] == -1 {
			break
		}
		userId := msg[0]
		qScore := msg[1]
		aScore := msg[2]
		//if qScore > float64(scoresAverage["questions"]) && aScore > float64(scoresAverage["answers"]) {
		item := &Item{
			id:       userId,
			priority: (qScore + aScore) * (-1), //the negative of the score
		}
		heap.Push(&pq, item)
		//}

		i++
	}
	log.Println("tamaño del heap", len(pq))
	nTop := 10
	if len(pq) < 10 {
		log.Println("hay menos de diez usuarios que cumplen: ", len(pq))
		nTop = len(pq)
	}

	for i := 0; i < nTop; i++ {
		log.Print("top ", i)
		item := heap.Pop(&pq).(*Item)
		log.Println(" id", item.id)
	}

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
}
