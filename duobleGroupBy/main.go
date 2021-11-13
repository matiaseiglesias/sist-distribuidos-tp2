package main

import (
	"strings"
	"sync"

	"github.com/jszwec/csvutil"
	"github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/conn"
	"github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/joinResult"
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
	log.Infof("input join results queue: %s", v.GetString("jr_input"))
	log.Infof("output queue: %s", v.GetString("groupByOutput"))
	log.Infof("Log Level: %s", v.GetString("log.level"))
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
		jQName := v.GetString("jr_input")
		outName := v.GetString("groupByOutput")
		endSignalOutput := v.GetString("endSignal")

		rabbitConn := conn.Init(addr)
		conn_, err := rabbitConn.Connect()
		conn.FailOnError(err, "Failed to connect to RabbitMQ")
		if !conn_ {
			log.Println("error while trying to connect to RabbitMQ, exiting...")
		}

		rabbitConn.RegisterQueues([]string{jQName, outName, endSignalOutput}, true)
		jMsgs, err := rabbitConn.Input(jQName)
		conn.FailOnError(err, "Failed to register a consumer")

		i := 0
		failures := 0
		var joinR_ []joinResult.JoinResult
		times := 1

		scoreTagsPerYear := make(map[int]map[string]int64)
		years := make([]int, 0)

		for jResult := range jMsgs {
			jResult.Ack(false)
			if err := csvutil.Unmarshal(jResult.Body, &joinR_); err != nil {
				log.Println("error:", err)
				failures++
				continue
			}
			if joinResult.IsEndResult(&joinR_[0]) {
				break
			}
			jYear := joinR_[0].CreationDate.Year()
			tags := strings.Split(joinR_[0].Tags, " ")
			score := joinR_[0].Score
			if !containYear(years, jYear) {
				scoreTagsPerYear[jYear] = make(map[string]int64)
				years = append(years, jYear)
			}
			for _, tag := range tags {
				scoreTagsPerYear[jYear][tag] += score
			}
			i++
		}

		for year, tagsScore := range scoreTagsPerYear {
			for tag, score := range tagsScore {
				tmpGB := []joinResult.GBResult{
					{
						Year:  year,
						Score: score,
						Tag:   tag,
					},
				}
				joinResult.GBPublish(rabbitConn.Channel, outName, tmpGB, times)
			}
		}

		endR := []joinResult.GBResult{joinResult.EndGBResult()}
		log.Println("sending endGBResult with id", endR[0].Id)
		joinResult.GBPublish(rabbitConn.Channel, endSignalOutput, endR, times)

		rabbitConn.Close()
		wg.Done()
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	wg.Wait()
	log.Printf(" [*] Closing input interface")
}
