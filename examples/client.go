package main

import (
	pigato "../"
	"log"
	"os"
	"time"
)

type message struct {
	Type, Text string
}

type Reply map[string]interface{}
type Request map[string]interface{}

func main() {
	var verbose bool
	if len(os.Args) > 1 && os.Args[1] == "-v" {
		verbose = true
	}
	session, _ := pigato.NewPigatoClient("tcp://127.0.0.1:55555", verbose)

	var rnum = 500

	start := time.Now()

	var answers int
	answers = 0

	var count int
	for count = 0; count < rnum; count++ {
		req := make(Request)
		req["m"] = message{"test", "foo"}
		session.Request("echo", req, func(rep interface{}) {
			answers++

			log.Printf("ANS %d %s", answers, rep["m"].FieldName("Type"))
			if answers == rnum {
				elapsed := time.Since(start)
				log.Printf("REQ took %s", elapsed)
			}
		})
	}

	time.Sleep(time.Second * 150)

}
