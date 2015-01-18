package raft

import (
	zmq "github.com/pebbe/zmq4"
	"strconv"
	//"fmt"
)

type Transport struct {
	RaftReqSocket	*zmq.Socket
	RaftRepSocket	*zmq.Socket
	clientRepSocket	*zmq.Socket
	raftPubSocket	*zmq.Socket
	raftSubSocket	*zmq.Socket
	host			string
	port			string
	name			string
	config			map[string][]string
}

func (t *Transport) Initialize(config map[string][]string, name string) {
	t.RaftRepSocket,_ = zmq.NewSocket(zmq.REP)
	t.clientRepSocket,_ = zmq.NewSocket(zmq.REP)
	//t.RaftReqSocket,_ = zmq.NewSocket(zmq.REQ)
	t.raftPubSocket,_ = zmq.NewSocket(zmq.PUB)
	t.raftSubSocket,_ = zmq.NewSocket(zmq.SUB)
	
	t.config = config
	info := config[name]
	port,_ := strconv.Atoi(info[5])
	//fsport := port + 4
	pubport := port +5
	raftport := port + 6
	clientport := port + 7
	host := info[4]
	t.raftPubSocket.Bind("tcp://"+host+":"+strconv.Itoa(pubport))
	t.clientRepSocket.Bind("tcp://"+host+":"+strconv.Itoa(clientport))
	t.RaftRepSocket.Bind("tcp://"+host+":"+strconv.Itoa(raftport))
	for _,v := range config {
		if v[0] == name {
			continue
		} else {
			port,_ := strconv.Atoi(v[5])
			t.raftSubSocket.Connect("tcp://"+v[4]+":"+strconv.Itoa(port+5))	
			t.raftSubSocket.SetSubscribe("")
		}
	}
}


func (t *Transport) Publish(data string) {
	t.raftPubSocket.Send(data,0)
}


/*func (t *Transport) ReceiveFromClient() string {
	data,_ := t.clientRepSocket.Recv(0)
	return data
}*/

func (t *Transport) SendToClient(data string) {
	t.clientRepSocket.Send(data,0)
}

func (t *Transport) AddSubscription(host, port string) {
	t.raftSubSocket.Connect("tcp://"+host+":"+port)	
	t.raftSubSocket.SetSubscribe("")
}

func (t *Transport) AddPeers(info []string) {
	t.config[info[0]] = info
}


