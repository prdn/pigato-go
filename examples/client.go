package main

import (
	pigato "../"
	"log"
	"os"
	"time"
)

type Reply struct {
	Type string
	Text string
}

type Request struct {
	Type string
	Text string
}

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
		req := Request{Type: "foo", Text: "bar"}
		session.Request("echo", req, &Reply{}, func(_rep interface{}) {
			answers++

			rep := _rep.(*Reply)

			log.Printf("%d) Type: %s | Text: %s", answers, rep.Type, rep.Text)
			if answers == rnum {
				elapsed := time.Since(start)
				log.Printf("REQ took %s", elapsed)
			}
		})
	}

	time.Sleep(time.Second * 150)

}
