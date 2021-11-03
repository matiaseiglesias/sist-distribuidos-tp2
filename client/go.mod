module github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/client

go 1.13

require (
	github.com/jszwec/csvutil v1.5.1
	github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/answers v0.0.0-00010101000000-000000000000
	github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/conn v0.0.0-00010101000000-000000000000
	github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/questions v0.0.0-00010101000000-000000000000
	github.com/pkg/errors v0.8.1
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/viper v1.9.0
	github.com/streadway/amqp v1.0.0
)

replace github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/questions => ../libraries/questions

replace github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/answers => ../libraries/answers

replace github.com/matiaseiglesias/sist-distribuidos-tp2/tree/master/libraries/conn => ../libraries/conn
