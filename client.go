package pigato

import (
	pgtlib "./lib"
	"encoding/json"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"log"
	"math/rand"
	"runtime"
	"time"
)

func randSeq() string {
	return fmt.Sprintf("%06X%06X", rand.Intn(0x10000), rand.Intn(0x10000))
}

type PigatoClient struct {
	broker   string
	identity string
	verbose  bool
	reqs     map[string]PigatoReq
	out      chan PigatoReq
	ctx      PigatoCtx
}

type PigatoReq struct {
	Reply interface{}
	Cb    PigatoHandler
	Msg   []string
}

type PigatoCtx struct {
	poller *zmq.Poller
	client *zmq.Socket
}

type PigatoHandler func(reply interface{})

func (pcli *PigatoClient) ConnectToBroker() (err error) {
	if pcli.ctx.client != nil {
		pcli.ctx.client.Close()
		pcli.ctx.client = nil
	}
	pcli.ctx.client, err = zmq.NewSocket(zmq.DEALER)
	pcli.identity = randSeq()
	pcli.ctx.client.SetIdentity(pcli.identity)

	if err != nil {
		if pcli.verbose {
			log.Println("E: ConnectToBroker() creating socket failed")
		}
		return
	}
	pcli.ctx.poller = zmq.NewPoller()
	pcli.ctx.poller.Add(pcli.ctx.client, zmq.POLLIN)

	if pcli.verbose {
		log.Printf("I: connecting to broker at %s...", pcli.broker)
	}
	err = pcli.ctx.client.Connect(pcli.broker)
	if err != nil && pcli.verbose {
		log.Println("E: ConnectToBroker() failed to connect to broker", pcli.broker)
	}

	return
}

func NewPigatoClient(broker string, verbose bool) (pcli *PigatoClient, err error) {

	pcli = &PigatoClient{
		broker:  broker,
		verbose: verbose,
	}
	err = pcli.ConnectToBroker()
	runtime.SetFinalizer(pcli, (*PigatoClient).Close)

	pcli.reqs = make(map[string]PigatoReq)
	pcli.out = make(chan PigatoReq, 100)

	go func() {
		for {
			pcli.Flush()
			time.Sleep(time.Millisecond * 10)
		}
	}()
	return
}

func (pcli *PigatoClient) Close() (err error) {
	if pcli.ctx.client != nil {
		err = pcli.ctx.client.Close()
		pcli.ctx.client = nil
	}
	return
}

func (pcli *PigatoClient) Request(service string, pld interface{}, rep interface{}, cb PigatoHandler) {
	req := make([]string, 6, 6)
	rid := pcli.identity + randSeq()
	val, _ := json.Marshal(pld)
	req[5] = ""
	req[4] = string(val)
	req[3] = rid
	req[2] = service
	req[1] = pgtlib.W_REQUEST
	req[0] = pgtlib.C_CLIENT

	if pcli.verbose {
		log.Printf("I: send request to '%s' service: %q\n", service, val)
	}

	preq := PigatoReq{Cb: cb, Msg: req, Reply: rep}
	pcli.reqs[rid] = preq
	pcli.out <- preq
}

func (pcli *PigatoClient) Flush() {
	for {
		evts, _ := pcli.ctx.client.GetEvents()
		if evts&zmq.POLLIN != 0 {
			msg := []string{}
			msg, err := pcli.ctx.client.RecvMessage(0)
			if msg == nil {
				break
			}

			if err != nil {
				log.Printf("%s", err)
				break
			}

			if len(msg) < 3 {
				break
			}

			if msg[0] != pgtlib.C_CLIENT {
				break
			}

			rid := msg[3]

			if req, exists := pcli.reqs[rid]; exists {
				delete(pcli.reqs, rid)
				if len(msg) >= 4 {
					_ = json.Unmarshal([]byte(msg[5]), req.Reply)
				}
				req.Cb(req.Reply)
			}
		} else {
			break
		}
	}

	var served bool
	served = false
	for {
		evts, _ := pcli.ctx.client.GetEvents()
		if evts&zmq.POLLOUT != 0 {
			select {
			case req := <-pcli.out:
				served = true
				_, _ = pcli.ctx.client.SendMessage(req.Msg)
			default:
				served = false
			}
		}
		if !served {
			break
		}

	}
}
