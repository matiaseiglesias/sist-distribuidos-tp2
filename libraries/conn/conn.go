package conn

import (
	"errors"
	"log"

	"github.com/jszwec/csvutil"
	"github.com/streadway/amqp"
)

const END_ID_NUMBER = -11118

type RabbitConn struct {
	Addr       string
	Connection *amqp.Connection
	Channel    *amqp.Channel
	Connected  bool
	Queues     []string
}

func Init(addr_ string) *RabbitConn {
	return &RabbitConn{
		Addr:      addr_,
		Connected: false,
	}
}
func (c *RabbitConn) Connect() (bool, error) {
	if c.Connected {
		return false, errors.New("conn already connected")
	}
	conn, err := amqp.Dial(c.Addr)
	if err != nil {
		return false, err
	}
	c.Connection = conn
	ch, err := conn.Channel()
	if err != nil {
		c.Connection.Close()
		return false, err
	}
	c.Channel = ch
	c.Connected = true

	return true, nil
}

func (c *RabbitConn) Close() {
	c.Channel.Close()
	c.Connection.Close()
}

func (c *RabbitConn) RegisterQueues(qNames []string, print bool) []string {

	for _, name := range qNames {
		_, err := c.Channel.QueueDeclare(
			name,  // name
			false, // durable
			false, // delete when unused
			false, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		if err != nil {
			if print {
				log.Println("Failed to register a queue", err)
			}
		} else {
			c.Queues = append(c.Queues, name)
		}
	}
	return c.Queues
}

func (c *RabbitConn) Input(qName string) (<-chan amqp.Delivery, error) {
	if !contains(c.Queues, qName) {
		return nil, errors.New("queue not declare")
	}
	return c.Channel.Consume(
		qName, // queue
		"",    // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
}

func (c *RabbitConn) Publish(qName string, b []byte) error {
	if !contains(c.Queues, qName) {
		return errors.New("queue not declare")
	}
	return c.Channel.Publish(
		"",    // exchange
		qName, // routing key
		false, // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         b,
		})

}

func FailOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func contains(set []string, item string) bool {

	for _, s := range set {
		if s == item {
			return true
		}
	}
	return false
}

type EndSync struct {
	ProcessName string `csv:"ProcessName"`
}

func (c *RabbitConn) SendEndSync(destiny, pName string, times int) error {

	if !contains(c.Queues, destiny) {
		return errors.New("queue not declare")
	}

	a := []EndSync{
		{
			ProcessName: pName,
		},
	}

	var err error
	data, err := csvutil.Marshal(a)
	if err != nil {
		return err
	}

	for i := 0; i < times; i++ {
		err = c.Channel.Publish(
			"",      // exchange
			destiny, // routing key
			false,   // mandatory
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         data,
			})
	}
	return err
}

type Delivery struct {
	Id             string
	currentIndex   int //:= 0
	maxIndex       int //:= len(groupQueuesAddr)
	ids            []float64
	groups         [][]string
	idToGroupQueue map[float64][]string
}

func containsInt(set []float64, value float64) bool {
	for _, v := range set {
		if v == value {
			return true
		}
	}
	return false
}

func (d *Delivery) Init(m map[string][]string) {
	d.idToGroupQueue = make(map[float64][]string)
	var groupAddr [][]string
	for _, val := range m {
		groupAddr = append(groupAddr, val)
	}
	d.maxIndex = len(groupAddr)
	d.groups = groupAddr
}

func (d *Delivery) GetOutQueueNames() []string {
	queues := []string{}
	for _, groups := range d.groups {
		queues = append(queues, groups...)
	}
	return queues
}

func (d *Delivery) GetGroup(id float64) []string {
	if !containsInt(d.ids, id) {
		d.ids = append(d.ids, id)
		d.idToGroupQueue[id] = d.groups[d.getNextIndex()]
	}
	return d.idToGroupQueue[id]

}

func (d *Delivery) getNextIndex() int {
	index := d.currentIndex
	d.currentIndex++
	if d.currentIndex == d.maxIndex {
		d.currentIndex = 0
	}
	return index
}
