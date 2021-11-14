package main

import (
	"container/heap"
	"strings"

	"github.com/jszwec/csvutil"
	"github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/joinResult"
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
	log.Infof("input double groupby results queue: %s", v.GetString("gb_input"))
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
	tag      string // The value of the item; arbitrary.
	priority int64  // The priority of the item in the queue.
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
func (pq *PriorityQueue) update(item *Item, value string, priority int64) {
	item.tag = value
	item.priority = priority
	heap.Fix(pq, item.index)
}

func containYear(set []int, value int) bool {
	for _, v := range set {
		if v == value {
			return true
		}
	}
	return false
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

	conn, err := amqp.Dial(addr)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	channel, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer channel.Close()

	gbName := v.GetString("gb_input")
	_, err = channel.QueueDeclare(
		gbName, // name
		false,  // durable
		false,  // delete when unused
		false,  // exclusive
		false,  // no-wait
		nil,    // arguments
	)
	failOnError(err, "Failed to register a consumer")

	gbMsgs, err := channel.Consume(
		gbName, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	nInputQueues := 1 // v.GetInt("total_input.n")
	finishedQueues := 0
	var gbR_ []joinResult.GBResult

	tagsScorePerYear := make(map[int]PriorityQueue)
	years := make([]int, 0)

	i := 0
	failures := 0

	for finishedQueues < nInputQueues {

		gbResult := <-gbMsgs

		if i%10000 == 0 {
			log.Println("[TOP 10 PTO3]Read questions: ", i)
			log.Println("[TOP 10 PTO3]fail questions: ", failures)
		}
		gbResult.Ack(false)

		if err := csvutil.Unmarshal(gbResult.Body, &gbR_); err != nil {
			log.Println("error:", err)
			failures++
			continue
		}
		if joinResult.IsEndGBResult(&gbR_[0]) {
			log.Println("end message received")
			finishedQueues++
			continue
		}

		item := &Item{
			tag:      gbR_[0].Tag,
			priority: gbR_[0].Score,
		}
		year := gbR_[0].Year
		tmpPQ := tagsScorePerYear[year]
		if !containYear(years, year) {
			heap.Init(&tmpPQ)
			years = append(years, year)
		}

		heap.Push(&tmpPQ, item)
		tagsScorePerYear[year] = tmpPQ

		gbR_ = nil
		i++
	}
	log.Println("starting processing top")
	for year_, pq_ := range tagsScorePerYear {
		nTop := 10
		if len(pq_) < 10 {
			//log.Println("hay menos de diez usuarios que cumplen: ", len(pq_))
			nTop = len(pq_)
		}
		log.Printf("Año %d, Top %d ", year_, nTop)
		for i_ := 1; i_ <= nTop; i_++ {
			item := heap.Pop(&pq_).(*Item)
			log.Printf("Top %d: Tag: %s, Score: %d", i_, item.tag, item.priority)
		}
	}
	log.Printf("Exiting")
}
