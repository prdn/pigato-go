package main

import (
	pigato "github.com/prdn/pigato-go"
	"log"
	"os"
	"pigato"
	"time"
)

type Request struct {
	S string `json:"s"`
}

type Reply struct {
	S string `json:"s"`
}

func main() {
	var verbose bool
	if len(os.Args) > 1 && os.Args[1] == "-v" {
		verbose = true
	}
	session, _ := pigato.NewPigatoClient("tcp://127.0.0.1:55555", verbose)

	var rnum = 50000

	start := time.Now()

	var answers int
	answers = 0

	var count int
	for count = 0; count < rnum; count++ {
		req := &Request{S: "hello"}

		session.Request("echo", req, func(reply interface{}) {
			answers++
			//log.Printf("ANS %d", answers)
			if answers == rnum {
				elapsed := time.Since(start)
				log.Printf("REQ took %s", elapsed)
			}
		})
	}

	time.Sleep(time.Second * 150)

}
