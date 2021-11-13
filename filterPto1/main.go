package main

import (
	"bytes"
	"encoding/binary"
	"strings"
	"sync"

	"github.com/grassmudhorses/vader-go/lexicon"
	"github.com/grassmudhorses/vader-go/sentitext"
	"github.com/jszwec/csvutil"
	"github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/answers"
	"github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/conn"
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
	log.Infof("rabbit queue address : %s", v.GetString("rabbitQueue.address"))
	log.Infof("rabbit queue input : %s", v.GetString("input"))
	log.Infof("rabbit queue output : %s", v.GetString("output"))
	log.Infof("end signal output queue : %s", v.GetString("endSignal.output.questions.qname"))
	log.Infof("Log Level: %s", v.GetString("log.level"))
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

	rabbitConn := conn.Init(addr)
	conn_, err := rabbitConn.Connect()
	conn.FailOnError(err, "Failed to connect to RabbitMQ")
	if !conn_ {
		log.Println("error while trying to connect to RabbitMQ, exiting...")
	}
	iA := v.GetString("input")
	oA := v.GetString("output")
	endASignalOutput := v.GetString("endSignal.output")
	queuesName := []string{iA, oA, endASignalOutput}

	rabbitConn.RegisterQueues(queuesName, true)

	msgs, err := rabbitConn.Input(iA)
	conn.FailOnError(err, "Failed to register a consumer")

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		i := 0
		mayor_10 := 0
		failures := 0
		negatives := 0
		var answers_ []answers.Answer
		for d := range msgs {
			if err := csvutil.Unmarshal(d.Body, &answers_); err != nil {
				log.Println("error:", err)
				failures++
				continue
			}
			d.Ack(false)
			if len(answers_) == 0 {
				failures++
				continue
			} else if answers.IsEndAnswer(&answers_[0]) {
				log.Println(" bye bye")
				break

			} else if answers_[0].Score > 10 {
				mayor_10++
				parseText := sentitext.Parse(answers_[0].Body, lexicon.DefaultLexicon)
				result := sentitext.PolarityScore(parseText)
				if result.Compound < -0.5 {
					negatives++
				}
			}
			if len(answers_) > 1 {
				log.Fatal("Se recibio mas de un mensaje!!!")
			}
			i += len(answers_)
			answers_ = nil
			if i%1 == 0 {
				log.Println("mensajes leidos: ", i)
				log.Println("mensajes mayores a diez: ", mayor_10)
				log.Println("mensajes fallidos : ", failures)
				log.Println("mensajes negativos : ", negatives)
			}
		}
		b := toByteArray(int64(negatives), int64(mayor_10))

		err = rabbitConn.Publish(oA, b)
		conn.FailOnError(err, "Failed to publish a message")

		endSignal := toByteArray(int64(-1), int64(-1))
		err = rabbitConn.Publish(endASignalOutput, endSignal)
		conn.FailOnError(err, "Failed to publish a message")

		wg.Done()
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	wg.Wait()
	log.Printf(" [*] closing filterPro1")
}
