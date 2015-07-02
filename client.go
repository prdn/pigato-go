package pigato

import (
	"encoding/json"
	"errors"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"log"
	"math/rand"
	"runtime"
	"sync"
	"time"
)

var (
	errPermanent = errors.New("permanent error, abandoning request")
)

func randSeq() string {
	return fmt.Sprintf("%04X-%04X", rand.Intn(0x10000), rand.Intn(0x10000))
}

type PigatoReq struct {
	Cb  PigatoHandler
	Msg []string
}

type PigatoClient struct {
	broker  string
	verbose bool          //  Print activity to stdout
	timeout time.Duration //  Request timeout
	reqs    map[string]PigatoReq
	//	out     *Queue
	out chan PigatoReq
	ctx PigatoCtx
}

type PigatoCtx struct {
	sync.Mutex
	poller *zmq.Poller
	client *zmq.Socket //  Socket to broker
}

type PigatoHandler func(reply interface{})

func (pcli *PigatoClient) ConnectToBroker() (err error) {
	if pcli.ctx.client != nil {
		pcli.ctx.client.Close()
		pcli.ctx.client = nil
	}
	pcli.ctx.client, err = zmq.NewSocket(zmq.DEALER)
	identity := randSeq()
	pcli.ctx.client.SetIdentity(identity)

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

//  ---------------------------------------------------------------------
//  Constructor

func NewPigatoClient(broker string, verbose bool) (pcli *PigatoClient, err error) {

	pcli = &PigatoClient{
		broker:  broker,
		verbose: verbose,
		timeout: time.Second,
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

//  ---------------------------------------------------------------------
//  Destructor

func (pcli *PigatoClient) Close() (err error) {
	if pcli.ctx.client != nil {
		err = pcli.ctx.client.Close()
		pcli.ctx.client = nil
	}
	return
}

//  ---------------------------------------------------------------------

//  Set request timeout.
func (pcli *PigatoClient) SetTimeout(timeout time.Duration) {
	pcli.timeout = timeout
}

func (pcli *PigatoClient) Request(service string, pld interface{}, cb PigatoHandler) {
	req := make([]string, 6, 6)
	rid := randSeq()
	val, _ := json.Marshal(pld)
	req[5] = ""
	req[4] = string(val)
	req[3] = rid
	req[2] = service
	req[1] = W_REQUEST
	req[0] = C_CLIENT
	if pcli.verbose {
		log.Printf("I: send request to '%s' service: %q\n", service, val)
	}
	preq := PigatoReq{Cb: cb, Msg: req}
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

			if msg[0] != C_CLIENT {
				break
			}

			rid := msg[3]

			req := pcli.reqs[rid]
			req.Cb(msg[5])
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