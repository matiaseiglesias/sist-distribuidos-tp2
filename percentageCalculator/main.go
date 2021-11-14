package main

import (
	"bytes"
	"encoding/binary"
	"strings"
	"sync"

	"github.com/jszwec/csvutil"
	"github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/conn"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type Result struct {
	TotalGrT10     int64   `csv:"TotalGrT10"`
	TotalNegatives int64   `csv:"TotalNegatives"`
	Percentage     float64 `csv:"Percentage"`
}

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
	output := v.GetString("output")
	rabbitConn.RegisterQueues([]string{iA, output}, true)

	msgs, err := rabbitConn.Input(iA)
	conn.FailOnError(err, "Failed to register a consumer")

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		i := 0
		failures := 0
		results := [][]int64{}
		for d := range msgs {

			msg := make([]int64, 2)
			buff := bytes.NewBuffer(d.Body)
			err2 := binary.Read(buff, binary.LittleEndian, msg)
			if err2 != nil {
				log.Println("binary.Read failed:", err2)
				failures++
				d.Ack(false)
				continue
			}
			d.Ack(false)
			if msg[0] == -1 && msg[1] == -1 {
				break
			}
			results = append(results, msg)
			i++
		}
		negatives := int64(0)
		greater10 := int64(0)
		for _, result := range results {
			negatives += result[0]
			greater10 += result[1]
		}
		log.Printf("final result, negatives > 10: %d, total > 10: %d \n", negatives, greater10)
		if greater10 > 0 {
			log.Println("final result, percentage: ", (float64(negatives)/float64(greater10))*100)
		} else {
			log.Println("final result, percentage: 0%")
		}

		finalResult := []Result{{TotalGrT10: greater10, TotalNegatives: negatives, Percentage: (float64(negatives) / float64(greater10)) * 100}}
		data, err := csvutil.Marshal(finalResult)
		if err != nil {
			log.Println("converting result failed:", err)
		}
		rabbitConn.Publish(output, data)

		rabbitConn.Close()
		wg.Done()
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	wg.Wait()
	log.Printf(" [*] Closing percentageCalculator")
}
