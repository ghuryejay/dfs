package raft

import (
	"time"
	"encoding/json"
	"strconv"
	"fmt"
	"os"
	"math/rand"
	"bufio"
	"strings"
	"sync"
	"io"
	"github.com/syndtr/goleveldb/leveldb"
	zmq "github.com/pebbe/zmq4"
)

type Server struct {
	sync.Mutex
	Pid			int
	LeaderPid	int
	Name		string
	transport	*Transport
	Term		int
	Votes		int
	VotedFor	string
	Logger		*Log
	Cluster		[]*Peer
	Timeout		int
	State		int
	Timer		int
	OldCount	int
	NewCount	int
	Majority	int
	config 		map[string][]string
	DBPath		string
	ClientRepChan	chan int
	LogFile		*os.File
}

var followerTimer *time.Ticker
var electionTimer *time.Ticker
var appendEntriesReplicationMap map[int]int
var appendEntriesMap	map[int]*RaftMessage
var appendEntriesId int
var mutex = &sync.Mutex{}
var catchupMutex = &sync.Mutex{}
var commitMutex = &sync.Mutex{}
var oldpeers, newpeers []string
var voteRecvMap = make(map[string]bool)

func (s *Server) NewServer(Name string, lines int) {
	inputFile, _ := os.Open("config")
    defer inputFile.Close()
    scanner := bufio.NewScanner(inputFile)
	configmap := make(map[string][]string)
	s.config = make(map[string][]string)
	cnt := 1
	for scanner.Scan() {
		if cnt > lines {
			break
		}
		configLine := scanner.Text()
		//fmt.Println(configLine)
		attrs := strings.Split(configLine, ",")
		configmap[attrs[0]] = attrs
		oldpeers = append(oldpeers,attrs[0])
		cnt++
	}
	s.config = configmap
	info := configmap[Name]
	s.Pid,_ = strconv.Atoi(info[1])
	s.Name = Name
	s.transport = new(Transport)
	s.transport.Initialize(configmap, Name)
	s.State = FOLLOWER
	s.Logger = new(Log)
	s.Timeout = random(200,500)
	s.Majority = (lines + 1)/2
	appendEntriesId = 1
	s.DBPath = "/tmp/"+s.Name + ".db"
	db, _ := leveldb.OpenFile(s.DBPath, nil)
	//fmt.Println("DB opened")
	term,_ := db.Get([]byte("Term"),nil)
	defer db.Close()

	s.LogFile,_ = os.Create("log-"+strconv.Itoa(s.Pid)+".txt")
	if term != nil {
		votedFor,_ := db.Get([]byte("VotedFor"),nil)
		log,_ := db.Get([]byte("Log"),nil)
		s.Term,_ = strconv.Atoi(string(term))
		s.VotedFor = string(votedFor)
		var entries []*Entry
		j := json.Unmarshal(log,&entries)
		if j == nil {
		}
		if len(entries) != 0 {
			for i := range entries {
				str,_ := json.MarshalIndent(entries[i], "", "  ")
				n,err := io.WriteString(s.LogFile,string(str)+"\n")
				if err != nil {
					fmt.Println(n,err)
				}
			}
			s.Logger.Entries = entries 
			s.Logger.Term = s.Term
			s.Logger.Index = entries[len(entries)-1].Index
		}
	}

	os.Mkdir(s.DBPath, os.ModeDir|0755)
	var names []string
	for k,_ := range configmap {
		names = append(names,k)
	}	
	for i := 0;i < lines;i++{
		if names[i] != Name {
			s.AddToCluster(names[i])
			oldpeers = append(oldpeers,names[i])
		}
	}
	s.OldCount = 0
	s.NewCount = 0
	appendEntriesReplicationMap = make(map[int]int)
	appendEntriesMap = make(map[int] *RaftMessage)
	s.ClientRepChan = make(chan int)
	s.StartReceiving()
}

func (s *Server) AddToCluster(id string){
	peer := new(Peer)
	//fmt.Println("Adding "+id)
	peer.Name = id
	peer.NextIndex = s.Logger.GetIndex()
	s.Cluster = append(s.Cluster,peer)
}

func (s *Server) Start() {
	s.run()
}

func random(min, max int) int {
    rand.Seed(time.Now().Unix())
    return rand.Intn(max - min) + min
}

func inold(peer string) bool {
	for i := range oldpeers {
		if oldpeers[i] == peer {
			return true
		}
	}
	return false
}

func innew(peer string) bool {
	for i := range newpeers {
		if newpeers[i] == peer {
			return true
		}
	}
	return false
}

func (s *Server) MessageListener(message RaftMessage) {
   
	if message.IValue == RAFT_APPEND_REQ {
			//electionTimer.Stop()
			electionTimer = time.NewTicker(time.Duration(s.Timeout) * time.Millisecond)
			if message.IsCatchup == true {
				s.HandleCatchup(message)
				return
			}
			resp := s.HandleAppendEntries(message)
			dataSend,_:= json.MarshalIndent(resp, "", "  ")
			if message.IsHeartBeat == true {
				s.transport.Publish(string(dataSend))
			} else {
				//fmt.Println("Sending REsp")
				if message.SizeChange == true {
				    fmt.Println("Size Change Request")
				     s.transport.Publish(string(dataSend))
				} else {
				    s.transport.Publish(string(dataSend))
				}
			}
	}
	
	if message.IValue == RAFT_APPEND_REP {
		//electionTimer.Stop()
		electionTimer = time.NewTicker(time.Duration(s.Timeout) * time.Millisecond)
		if message.LeaderId == s.Name {
			electionTimer = time.NewTicker(time.Duration(s.Timeout) * time.Millisecond)
			var str string
			if message.Success == true {
				str = "true"
			} else {
				str = "false"
			}
			if message.IsHeartBeat == false {
				if message.Success == true {
					if message.Success {
					     commitMutex.Lock()
                         defer commitMutex.Unlock()
                         id := strconv.Itoa(message.Id)
                         if _,ok := voteRecvMap[message.Name+id];!ok {
                         	 fmt.Println("RECV APPEND_REP Success "+str)
                         	 voteRecvMap[message.Name+id] = true
		                     if _,ok := appendEntriesMap[message.Id];ok {
								 appendEntriesReplicationMap[message.Id]++
								 if  appendEntriesReplicationMap[message.Id] >= s.Majority {
				                    entryreq := appendEntriesMap[message.Id]
				                    if entryreq != nil {
				                        for i := range entryreq.Entries {
				                            fmt.Println("COMMIT Term "+strconv.Itoa(entryreq.Entries[i].Term)+" Index "+strconv.Itoa(entryreq.Entries[i].Index))
				                            str,_ := json.MarshalIndent(entryreq.Entries[i], "", "  ")
				                            n,err := io.WriteString(s.LogFile,string(str)+"\n")
				                            if err != nil{
				                                fmt.Println(n,err)
				                            }
				                        }
				                    }
				                    repMsg := new(RaftMessage)
				                    repMsg.Name = s.Name
				                    repMsg.Success = true
				                    repMsg.LeaderPid = s.Pid
				                    repMsg.IValue = RAFT_CLIENT_VALUE_REPLY
				                    dataSend,_:= json.MarshalIndent(repMsg, "", "  ")
				                    delete(appendEntriesMap,message.Id)
				                    delete(appendEntriesReplicationMap,message.Id)
				                    for i := range s.Cluster {
				                        s.Cluster[i].NextIndex = s.Logger.GetIndex()
				                    }
				                    s.saveState()
				                    s.transport.clientRepSocket.Send(string(dataSend),0)
				               }
		                   }
                       }
					}
				} else {
					if message.WasTermStale == true {
						s.setTerm(message.Term)
					} else {
						var peer *Peer
						//fmt.Println(message.Name)
						for i := range s.Cluster {
							if s.Cluster[i].Name == message.Name {
								peer = s.Cluster[i]
								break
							}
						}
						//fmt.Println("Start check")
						s.startCatchUp(peer,message.Id)
					}
				}
			} 
			if message.SizeChange == true {
				if inold(message.Name) == true {
					s.OldCount++
				} else {
					if innew(message.Name) == true {
						s.NewCount++
					}
				}
				if s.OldCount == 2 && s.NewCount == 1{
					s.ClientRepChan <- 2
				}
			}
		}
	}
	
	if message.IValue == RAFT_VOTE_REQ {
		//electionTimer.Stop()
		fmt.Println("RECV VOTE_REQ From "+message.CandidateId)
		electionTimer = time.NewTicker(time.Duration(s.Timeout) * time.Millisecond)
		voteresp := s.OnElectionRequestReceived(message)
		dataSend,_:= json.MarshalIndent(voteresp, "", "  ")
		s.transport.Publish(string(dataSend))
	}
	
	if message.IValue == RAFT_VOTE_REP {
		//electionTimer.Stop()
		if message.CandidateId == s.Name {
			electionTimer = time.NewTicker(time.Duration(s.Timeout) * time.Millisecond)
			var str string
			if message.Success == true {
				str = "true"
			} else {
				str = "false"
			}
			fmt.Println("RECV VOTE_REP from "+message.Name+" "+str)
			s.onVoteResponseReceived(message)
		}
	}
	if message.IValue == RAFT_CLIENT_SIZE_REQ {
		//fmt.Println(s.LeaderPid)
		if s.LeaderPid != s.Pid {
			clientReply := new(RaftMessage)
			clientReply.Success = false
			clientReply.IValue = s.LeaderPid
			dataSend,_:= json.MarshalIndent(clientReply, "", "  ")
			s.transport.SendToClient(string(dataSend))
		} else {
			fmt.Println("RECV SIZE_REQ from client")
			s.AddpeersFromConfig(message.IValue)
			appendEntryreq := new(RaftMessage)
			appendEntryreq.SizeChange = true
			appendEntryreq.Size = message.IValue
			appendEntryreq.IValue = RAFT_APPEND_REQ
			appendEntryreq.Term = s.Term
			appendEntryreq.LeaderId = s.Name
			appendEntryreq.PrevLogIndex = s.Logger.GetIndex()
			appendEntryreq.PrevLogTerm = s.Logger.GetTerm()
			entry := new(Entry)
			entry.Vtype = 2
			entry.Index = s.Logger.GetIndex()+1
			entry.Term = s.Term
			entry.Size = 2
			entry.Value = "size change"
			appendEntryreq.Entries = append(appendEntryreq.Entries,entry)
			dataSend,_:= json.MarshalIndent(appendEntryreq, "", "  ")
			b := s.Logger.Append(entry)
			if b == false {
			}
			s.transport.Publish(string(dataSend))
			go func() {
				<- s.ClientRepChan
				s.OldCount = 0
				s.NewCount = 0
				s.writeToLog()
				clientReply := new(RaftMessage)
				clientReply.Success = true
				clientReply.IValue = RAFT_CLIENT_SIZE_REPLY
				dataSend,_:= json.MarshalIndent(clientReply, "", "  ")
				s.transport.SendToClient(string(dataSend))
			}()
		}
	}
	if message.IValue == RAFT_CLIENT_VALUE_REQ {
		if s.LeaderPid != s.Pid {
			clientReply := new(RaftMessage)
			clientReply.Success = false
			clientReply.IValue = s.LeaderPid
			dataSend,_:= json.MarshalIndent(clientReply, "", "  ")
			s.transport.SendToClient(string(dataSend))
		} else {
			fmt.Println("RECV VALUE_REQ from client")
			appendEntryReq := new(RaftMessage)
			appendEntryReq.Id = appendEntriesId
			appendEntriesId++
			appendEntryReq.LeaderId = s.Name
			appendEntryReq.Term = s.Term
			appendEntryReq.IValue = RAFT_APPEND_REQ
			appendEntryReq.PrevLogIndex = s.Logger.GetPrevLogIndex()
			var k int
			if s.Logger.GetPrevLogTerm() == 0 {
				k = s.Term
			} else {
				k = s.Logger.GetPrevLogTerm()
			}
			appendEntryReq.PrevLogTerm = k
			appendEntryReq.IsHeartBeat = false
			for i := range message.Entries {
				message.Entries[i].Vtype = 3
				message.Entries[i].Term = s.Term
				message.Entries[i].Index = s.Logger.GetIndex()+1
				b := s.Logger.Append(message.Entries[i])
				if b == false {
				}
			}
		
			appendEntryReq.Entries = message.Entries
			dataSend,_:= json.MarshalIndent(appendEntryReq, "", "  ")
			commitMutex.Lock()
            defer commitMutex.Unlock()
			appendEntriesMap[appendEntryReq.Id] = appendEntryReq
			appendEntriesReplicationMap[appendEntryReq.Id] = 1
			fmt.Println("SENT APPEND_REQ from "+s.Name)
			s.transport.Publish(string(dataSend))
		}
	}
}


func (s *Server) checkRetries() {
    commitMutex.Lock()
    defer commitMutex.Unlock()
	for k,_ := range appendEntriesMap {
	    if appendEntriesReplicationMap[k] < s.Majority {
            entryreq := appendEntriesMap[k]
            dataSend,_:= json.MarshalIndent(entryreq, "", "  ")
            s.transport.Publish(string(dataSend))
        }
    }
}


func (s *Server) startElection() {
	s.State = CANDIDATE
	s.Votes ++
	voteReq := new(RaftMessage)
	voteReq.IValue = RAFT_VOTE_REQ
	voteReq.Term = s.Term
	voteReq.CandidateId = s.Name
	voteReq.PrevLogIndex = s.Logger.GetIndex()
	voteReq.PrevLogTerm = s.Logger.GetTerm()
	voteReqToSend,_:= json.MarshalIndent(voteReq, "", "  ")
	fmt.Println("SENT VOTE_REQ term "+strconv.Itoa(s.Term))
	s.transport.Publish(string(voteReqToSend))
}

func (s *Server) StartReceiving() {
	go s.listenSubscription()
	go s.listenToClient()
	go s.listenToRaftServer()
	//go s.checkCommits()
}

func (s *Server) listenSubscription(){
	for {
		data,_ := s.transport.raftSubSocket.Recv(0)
		var msg RaftMessage
		j := json.Unmarshal([]byte(data),&msg)
		if j == nil{
		}
		go s.MessageListener(msg)
	}
}

func (s *Server) listenToClient() {
	for {
		data,_ := s.transport.clientRepSocket.Recv(0)
		var msg RaftMessage
		j := json.Unmarshal([]byte(data),&msg)
		if j == nil{
		}
		s.MessageListener(msg)
	}
}

func (s *Server) listenToRaftServer() {
	for {
		data,_ := s.transport.RaftRepSocket.Recv(0)
		if data != "" {
			var msg RaftMessage
			j := json.Unmarshal([]byte(data),&msg)
			if j == nil{
			}
			//fmt.Println("Catchup Received")
			s.HandleCatchup(msg)
		}
	}
}


func (s *Server) run() {
	electionTimer = time.NewTicker(time.Duration(s.Timeout) * time.Millisecond)
	followerTimer = time.NewTicker(50*time.Millisecond)
	commitTimer := time.NewTicker(2000*time.Millisecond)
	for {
		select {
				
			case <- electionTimer.C:
					s.electionTimeout()
			
			case <- followerTimer.C:
				if s.State == LEADER {
					s.sendHeartBeat()
					
				}
			case <- commitTimer.C:
			     if s.State == LEADER {
			        s.checkRetries()
			    }
		}
	}
}

func (s *Server) sendHeartBeat() {
	heartbeat := new(RaftMessage)
	heartbeat.IValue = RAFT_APPEND_REQ
	heartbeat.Term = s.Term
	heartbeat.LeaderId = s.Name
	heartbeat.LeaderPid = s.Pid
	entry := new(Entry)
	entry.Vtype = 1
	entry.Term = s.Term
	entry.Value = s.Name + strconv.Itoa(s.Timer)
	heartbeat.Name = s.Name
	heartbeat.Entries = append(heartbeat.Entries,entry)
	heartbeat.IsHeartBeat = true
	s.Timer++
	dataSend,_:= json.MarshalIndent(heartbeat, "", "  ")
	s.transport.Publish(string(dataSend))
}

func (s *Server) startCatchUp(peer *Peer,id int) {
	catchupMutex.Lock()
	defer catchupMutex.Unlock()
	for {
			if peer == nil {
				fmt.Println("Peer is nil")
				return
			}
			//fmt.Println("Catchup started")
			entries,prevLogIndex,prevLogTerm := s.Logger.GetEntryForRequest(peer.NextIndex)
			aer := new(RaftMessage)
			aer.Term = s.Term
			aer.LeaderId = s.Name
			aer.Name = s.Name
			aer.PrevLogIndex = prevLogIndex
			aer.PrevLogTerm = prevLogTerm
			if entries != nil{
				for i := range entries {
				aer.Entries = append(aer.Entries,entries[i])
				}
			}
			aer.IValue = RAFT_APPEND_REQ
			aer.IsCatchup = true
			if peer.NextIndex == 1 {
				aer.IsComplete = true
			}
			dataSend,_:= json.MarshalIndent(aer, "", "  ")
			//fmt.Println("Sending REq to "+peer.Name)
			info := s.config[peer.Name]
			host := info[4]
			port,_ := strconv.Atoi(info[5])
			s.transport.RaftReqSocket,_ = zmq.NewSocket(zmq.REQ)
			s.transport.RaftReqSocket.Connect("tcp://"+host+":"+strconv.Itoa(port+6))
			defer s.transport.RaftReqSocket.Close()
			s.transport.RaftReqSocket.Send(string(dataSend),0)
			rep,_ := s.transport.RaftReqSocket.Recv(0)
			electionTimer = time.NewTicker(time.Duration(s.Timeout) * time.Millisecond)
			var msg RaftMessage
			j := json.Unmarshal([]byte(rep),&msg)
			if j == nil{
			}
			if msg.Success == false {
				peer.NextIndex--
				if peer.NextIndex < 1 {
					peer.NextIndex = 1
				}
				continue
			} else {
				if _,ok := appendEntriesMap[id];ok{
					appendEntriesReplicationMap[id]++
					if appendEntriesReplicationMap[id] >= s.Majority {
						entryreq := appendEntriesMap[id]
		                if entryreq != nil {
		                    for i := range entryreq.Entries {
		                        fmt.Println("COMMIT Term "+strconv.Itoa(entryreq.Entries[i].Term)+" Index "+strconv.Itoa(entryreq.Entries[i].Index))
		                        str,_ := json.MarshalIndent(entryreq.Entries[i], "", "  ")
		                        n,err := io.WriteString(s.LogFile,string(str)+"\n")
		                        if err != nil{
		                            fmt.Println(n,err)
		                        }
		                    }
		                }
						delete(appendEntriesReplicationMap,id)
						delete(appendEntriesMap,id)
						repMsg := new(RaftMessage)
                        repMsg.Name = s.Name
                        repMsg.Success = true
                        repMsg.LeaderPid = s.Pid
                        repMsg.IValue = RAFT_CLIENT_VALUE_REPLY
                        dataSend,_:= json.MarshalIndent(repMsg, "", "  ")
                        s.transport.clientRepSocket.Send(string(dataSend),0)
					}
				}
				peer.NextIndex = s.Logger.GetIndex()
				break
			}
	}
}

func (s *Server) HandleCatchup(message RaftMessage){
	retMsg := new(RaftMessage)
	//fmt.Println("Handling Catchups")
	if message.Entries == nil {
		retMsg.Success =false
		retMsg.IsCatchup = true
		dataSend,_ := json.MarshalIndent(retMsg, "", "  ")
		s.transport.RaftRepSocket.Send(string(dataSend),0)
		//fmt.Println("First failed")
		return
	}
	
	valid := s.Logger.Check(message.PrevLogIndex, message.PrevLogTerm, message.PrevLogIndex + 1, message.Term)
	if message.IsComplete == true {
		valid = true
	}
	if valid == false {
		retMsg.Success =false
		//fmt.Println("Second failed")
		retMsg.IsCatchup = true
	} else {
		for i := range message.Entries {
			b := s.Logger.Append(message.Entries[i])
			if b == true {
				str,_ := json.MarshalIndent(message.Entries[i], "", "  ")
				n,err := io.WriteString(s.LogFile,string(str)+"\n")
				if err != nil{
					fmt.Println(n,err)
				}
			}
		}
		retMsg.Success = true
		retMsg.IsCatchup = true
	}
	if retMsg.Success == true {
		ent := s.Logger.Entries[len(s.Logger.Entries)-1]
		if ent.Vtype == 2 {
			appendEntryResponse := new(RaftMessage)
			appendEntryResponse.Term = s.Term
			appendEntryResponse.IValue = RAFT_APPEND_REP
			appendEntryResponse.Name = s.Name
			appendEntryResponse.Success = true
			appendEntryResponse.LeaderId = message.LeaderId
			appendEntryResponse.SizeChange = true
			appendEntryResponse.WasTermStale = false
			//fmt.Println(appendEntryResponse)
			s.saveState()
			dataSend,_ := json.MarshalIndent(appendEntryResponse, "", "  ")
			s.transport.Publish(string(dataSend))
			//appendEntryResponse.Id = message.Id
		}
	}
	s.saveState()
	dataSend,_ := json.MarshalIndent(retMsg, "", "  ")
	s.transport.RaftRepSocket.Send(string(dataSend),0)
}


func (s *Server) OnElectionRequestReceived(voteRequest RaftMessage) *RaftMessage {
	s.Lock()
	defer s.Unlock()
	if voteRequest.Term < s.Term {
		//fmt.Println("Sending false resp")
		voteResponse := new(RaftMessage)
		voteResponse.Term = s.Term
		voteResponse.IValue = RAFT_VOTE_REP
		voteResponse.Success = false
		voteResponse.Name = s.Name
		voteResponse.CandidateId = voteRequest.CandidateId
		return voteResponse
	}
	
	if voteRequest.Term > s.Term {
		//fmt.Println("End Election")
		s.setTerm(voteRequest.Term)
	}
	
	if s.VotedFor != "" && s.VotedFor != voteRequest.CandidateId {
		voteResponse := new(RaftMessage)
		voteResponse.Term = s.Term
		voteResponse.IValue = RAFT_VOTE_REP
		voteResponse.Success = false
		voteResponse.Name = s.Name
		voteResponse.CandidateId = voteRequest.CandidateId
		s.saveState()
		return voteResponse
	}
	
	if s.Logger.IsFresher(voteRequest.Term,voteRequest.Index) {
		voteResponse := new(RaftMessage)
		voteResponse.Term = s.Term
		voteResponse.IValue = RAFT_VOTE_REP
		voteResponse.Success = false
		voteResponse.Name = s.Name
		voteResponse.CandidateId = voteRequest.CandidateId
		s.saveState()
		return voteResponse
	}
	
	s.VotedFor = voteRequest.CandidateId
	voteResponse := new(RaftMessage)
	voteResponse.Term = s.Term
	voteResponse.IValue = RAFT_VOTE_REP
	voteResponse.Success = true
	voteResponse.Name = s.Name	
	voteResponse.CandidateId = voteRequest.CandidateId
	s.saveState()
	return voteResponse
}

func (s *Server) saveState() {
	mutex.Lock()
	defer mutex.Unlock()
	db, err1 := leveldb.OpenFile(s.DBPath, nil)
	defer db.Close()
	if err1 != nil {
		return 
	}
	err := db.Put([]byte("Term"),[]byte(strconv.Itoa(s.Term)),nil)
	err = db.Put([]byte("VotedFor"),[]byte(s.VotedFor),nil)
	logstr,_ := json.MarshalIndent(s.Logger.Entries, "", "  ")
	err = db.Put([]byte("Log"),[]byte(logstr),nil)
	if err == nil {
	}
}

func (s *Server) setTerm(term int) {
	
	if term < s.Term {
		return
	}
	//fmt.Println("Term set to follower")
	s.Term = term
	s.State = FOLLOWER
	s.VotedFor = ""
	s.Votes = 0
}

func (s *Server) nextTerm() {
	
	s.Term++
	s.State = FOLLOWER
	s.VotedFor = ""
	s.Votes = 0
}

func (s *Server) stepDown() {
	
	if s.State == FOLLOWER {
		return 
	}
	s.State = FOLLOWER
	s.VotedFor = ""
	s.Votes = 0
}

func (s *Server) makeLeader() {
	s.State = LEADER
	for i := range s.Cluster {
		s.Cluster[i].NextIndex = s.Logger.GetIndex()
	}
	s.LeaderPid = s.Pid
	s.sendHeartBeat()
	fmt.Println("Leader Made")
}

func (s *Server) electionTimeout() {
	if s.State == LEADER {
		return
	}	
	s.nextTerm()
	s.startElection()
}

func (s *Server) onVoteResponseReceived(voteResponse RaftMessage) {
	s.Lock()
	defer s.Unlock()
	if s.State != CANDIDATE {
		return
	}
	
	if voteResponse.Term != s.Term {
		if voteResponse.Term > s.Term {
			s.setTerm(voteResponse.Term)
		}
	}
	
	if voteResponse.Success == true {
		s.onVoteGranted()
	}
}

func (s *Server) onVoteGranted() {
	
	s.Votes++
	majority := s.Majority
	if s.Votes >= majority {
		s.sendHeartBeat()
		s.makeLeader()
		s.Votes = 0
	}
}

func (s *Server) AddpeersFromConfig(lim int) {
	inputFile, _ := os.Open("config")
	defer inputFile.Close()
	scanner := bufio.NewScanner(inputFile)
	config := make(map[string][]string)
	cnt := 0
	for scanner.Scan() {
		if cnt > 5{
			break
		}
		if cnt < 3 {
			cnt++
			continue
		}
		configLine := scanner.Text()
		attrs := strings.Split(configLine, ",")
		config[attrs[0]] = attrs
		if attrs[0] != s.Name {
			host:= attrs[4]
			port := attrs[5]
			portNo,_ := strconv.Atoi(port)
			s.transport.AddSubscription(host,strconv.Itoa(portNo+5))
		//	fmt.Println("added "+attrs[0])
			s.transport.AddPeers(attrs)
			found := false
			for i := range s.Cluster {
				if s.Cluster[i].Name == attrs[0] {
					if found == true {
						break
					}
				}
			}
			if found == false {
				newpeers = append(newpeers,attrs[0])
			}
			s.AddToCluster(attrs[0])
		}
		cnt++
	}
	s.Majority = 3
}

func(s *Server) HandleAppendEntries(appendEntryReq RaftMessage) *RaftMessage{
		
	appendEntryResponse := new(RaftMessage)
	if s.State != FOLLOWER {
		s.stepDown()
	}
	
	if appendEntryReq.Term > s.Term {
		s.setTerm(appendEntryReq.Term)
	}
	
	if appendEntryReq.Term < s.Term {
		//fmt.Println("APpend term: "+strconv.Itoa(appendEntryReq.Term))
		//.Println("server term: "+strconv.Itoa(s.Term))
		appendEntryResponse.Term = s.Term
		appendEntryResponse.Name = s.Name
		appendEntryResponse.Success = false
		appendEntryResponse.WasTermStale = true
		appendEntryResponse.IValue = RAFT_APPEND_REP
		appendEntryResponse.Id = appendEntryReq.Id
		s.saveState()
		//fmt.Println("Responding false")
		return appendEntryResponse
	}
	
	if appendEntryReq.IsHeartBeat == true {
		s.State = FOLLOWER
		s.LeaderPid = appendEntryReq.LeaderPid
		appendEntryResponse.Term = s.Term
		appendEntryResponse.Name = s.Name
		appendEntryResponse.IValue = RAFT_APPEND_REP
		appendEntryResponse.LeaderId = appendEntryReq.LeaderId
		appendEntryResponse.Success = true
		appendEntryResponse.WasTermStale = false
		s.LeaderPid = appendEntryReq.LeaderPid
		appendEntryResponse.Id = appendEntryReq.Id
		appendEntryResponse.IsHeartBeat = true
		return appendEntryResponse	
	}

	
	//fmt.Println("RECVD APPEND_REQ from "+appendEntryReq.LeaderId+" term "+strconv.Itoa(appendEntryReq.Term))
	valid := s.Logger.Check(appendEntryReq.PrevLogIndex, appendEntryReq.PrevLogTerm, appendEntryReq.PrevLogIndex + 1, appendEntryReq.Term)
	//fmt.Println("PrevLogIndex: "+strconv.Itoa(appendEntryReq.PrevLogIndex))
	//fmt.Println("PrevLogTerm: "+strconv.Itoa(appendEntryReq.PrevLogTerm))
	if valid == false {
		appendEntryResponse.Term = s.Term
		appendEntryResponse.IValue = RAFT_APPEND_REP
		appendEntryResponse.Name = s.Name
		appendEntryResponse.LeaderId = appendEntryReq.LeaderId
		appendEntryResponse.Success = false
		appendEntryResponse.WasTermStale = false
		appendEntryResponse.Id = appendEntryReq.Id
	//	fmt.Println("Responding false due to check func")
		s.saveState()
		return appendEntryResponse
	}
	if appendEntryReq.SizeChange == true {
		
		s.AddpeersFromConfig(appendEntryReq.Size)
		for i := range appendEntryReq.Entries {
			b := s.Logger.Append(appendEntryReq.Entries[i])
			if b == false {
			}
		}
	
		appendEntryResponse.Term = s.Term
		appendEntryResponse.IValue = RAFT_APPEND_REP
		appendEntryResponse.Name = s.Name
		appendEntryResponse.Success = true
		appendEntryResponse.LeaderId = appendEntryReq.LeaderId
		appendEntryResponse.SizeChange = true
		appendEntryResponse.WasTermStale = false
		appendEntryResponse.Id = appendEntryReq.Id
		s.saveState()
		s.writeToLog()
		return appendEntryResponse
	}
	
	for j := range appendEntryReq.Entries{
		entry := new(Entry)
		entry.Vtype = 3
		entry.Index = appendEntryReq.PrevLogIndex + 1
		entry.Term = appendEntryReq.Term
		entry.Value = appendEntryReq.Entries[j].Value
		b := s.Logger.Append(entry)
		if b == true{
			s.writeToLog()
		}
	}
	
	appendEntryResponse.Term = s.Term
	appendEntryResponse.Name = s.Name
	appendEntryResponse.LeaderId = appendEntryReq.LeaderId
	appendEntryResponse.IValue = RAFT_APPEND_REP
	appendEntryResponse.Success = true
	appendEntryResponse.WasTermStale = false
	appendEntryResponse.Id = appendEntryReq.Id
	s.saveState()
	return appendEntryResponse
}

func(s *Server) writeToLog() {
	s.LogFile,_ = os.Create("log-"+strconv.Itoa(s.Pid)+".txt")
	for i := range s.Logger.Entries {
		entry := s.Logger.Entries[i]
		str,_ := json.MarshalIndent(entry, "", "  ")
		n,err := io.WriteString(s.LogFile,string(str)+"\n")
		if err != nil{
			fmt.Println(n,err)
		}
	}
}

