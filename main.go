package main

import (
	"fmt"
)

func reverse_int(n int64) int64 {
	new_int := int64(0)
	for n > 0 {
		remainder := n % 10
		new_int *= 10
		new_int += remainder
		n /= 10
	}
	return new_int
}

func main() {

	fmt.Println("nadie en el canal: ", reverse_int(1234))
	fmt.Println("nadie en el canal: ", reverse_int(0))
	fmt.Println("nadie en el canal: ", reverse_int(125478))

	//var chans []chan bool
	//
	//chans = append(chans, make(chan bool))
	//chans = append(chans, make(chan bool))
	//chans = append(chans, make(chan bool))
	//
	//go func() {
	//	time.Sleep(3 * time.Second)
	//	chans[2] <- false
	//
	//}()
	//
	//running := true
	//
	//for running {
	//	for i := 0; i < 3; i++ {
	//		select {
	//		case c := <-chans[i]:
	//			fmt.Println("Me llego una señal del canal n: ", i)
	//			running = c
	//		default:
	//			fmt.Println("nadie en el canal: ", i)
	//			time.Sleep(500 * time.Millisecond)
	//		}
	//	}
	//}
} //
