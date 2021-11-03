module github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/topTenPto3

go 1.13

require (
	github.com/fatih/structs v1.1.0
	github.com/grassmudhorses/vader-go v0.0.0-20191126145716-003d5aacdb71
	github.com/jszwec/csvutil v1.5.1
	github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/joinResult v0.0.0-00010101000000-000000000000
	github.com/mitchellh/mapstructure v1.4.2
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/viper v1.9.0
	github.com/streadway/amqp v1.0.0
)

replace github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/questions => ../libraries/questions

replace github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/joinResult => ../libraries/joinResult