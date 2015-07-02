package pigato

const (
	C_CLIENT = "C"
	W_WORKER = "W"
)

const (
	W_READY         = "1"
	W_REQUEST       = "2"
	W_REPLY         = "3"
	W_HEARTBEAT     = "4"
	W_DISCONNECT    = "5"
	W_REPLY_PARTIAL = "6"
)

var (
	MDPS_COMMANDS = map[string]string{
		W_READY:      "READY",
		W_REQUEST:    "REQUEST",
		W_REPLY:      "REPLY",
		W_HEARTBEAT:  "HEARTBEAT",
		W_DISCONNECT: "DISCONNECT",
	}
)
