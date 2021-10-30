package questions

import "time"

type Question struct {
	Id           int64     `csv:"Id"`
	OwnerUserId  float64   `csv:"OwnerUserId"`
	CreationDate time.Time `csv:"CreationDate"`
	ClosedDate   string    `csv:"ClosedDate"`
	Score        int64     `csv:"Score"`
	Title        string    `csv:"Title"`
	Body         string    `csv:"Body"`
	Tags         string    `csv:"Tags"`
}
