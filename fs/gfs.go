// memfs implements a simple in-memory file system.
package main

/*
 Two main files are ../fuse.go and ../fs/serve.go
*/

import (
	"fmt"
	"flag"
	"log"
	"os"
	"net"
	"sync"
	"time"
	"bufio"
	"strings"
	"strconv"
	"encoding/json"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"dss/p4_jghurye/dfs"
	"dss/p5jghurye/raft/core"
	"github.com/syndtr/goleveldb/leveldb"
	zmq "github.com/pebbe/zmq4"
)

//=============================================================================

type Node struct {
	//Vid        int
	Name       string
	Attrs      fuse.Attr
	dirty      bool
	metaDirty  bool
	parent     *Node
	ParentSig  string
	kids       map[string]*Node
	ChildVids  map[string]string
	data       []byte
	DataBlocks []string
	FlushTime	time.Time
	isExpanded	bool
	Path		string
}

type Message struct {
	From string
	Nodes map[string]*Node
	Root *Node
}

type DataMessageReq struct {
		
	From string
	NodeHash string
	DataReq	bool
}

type DataMessageRep struct {

	From string
	NodeHash string
	Node *Node
	DataMap  map[string][]byte
	DataBlocks	[]string
}

var root, head *Node
var nextInd uint64
var nodeMap = make(map[uint64]*Node) // not currently queried
var debug = true
var uid = os.Geteuid()
var gid = os.Getegid()
var b int
var publisher, subscriber, demandServer *zmq.Socket
var nodes = make(map[string]*Node)
var pathmap = make(map[string]*Node)
var destmap = make(map[string]string)
var pidmap = make(map[int][]string)
var leaderpid int

type FS struct{}

var vidInd int
var mutex = &sync.Mutex{}
var dbRoot string
var mountPoint string
var name string
var config map[string][]string
var curSource string


//=============================================================================

func p_out(s string, args ...interface{}) {
	if !debug {
		return
	}
	log.Printf(s, args...)
}

func p_err(s string, args ...interface{}) {
	log.Printf(s, args...)
}

//=============================================================================

func (FS) Root() (fs.Node, fuse.Error) {
	//p_out("root returns as %d\n", int(root.Attrs.Inode))
	return root, nil
}

//=============================================================================

func (n *Node) isDir() bool {
	return (n.Attrs.Mode & os.ModeDir) != 0
}

func (n *Node) fuseType() fuse.DirentType {
	if n.isDir() {
		return fuse.DT_Dir
	} else {
		return fuse.DT_File
	}
}

func (n *Node) init(filename string, mode os.FileMode) {
	nextInd++
	n.Attrs.Inode = nextInd
	n.Attrs.Nlink = 1
	n.Name = filename
	leaderpid = -1
	tm := time.Now()
	n.Attrs.Atime = tm
	n.Attrs.Mtime = tm
	n.Attrs.Ctime = tm
	n.Attrs.Crtime = tm
	n.Attrs.Mode = mode

	n.Attrs.Gid = uint32(gid)
	n.Attrs.Uid = uint32(uid)

	n.kids = make(map[string]*Node)
	n.ChildVids = make(map[string]string)
	nodeMap[nextInd] = n
	n.isExpanded = false
	n.Path = ""
	p_out("inited node inode %d, %q\n", nextInd, name)
}

func (n *Node) Attr() fuse.Attr {
	n = pathmap[n.Path]
	p_out("Attr() on %q (%d)\n", n.Name, n.Attrs.Inode)
	return n.Attrs
}

func (n *Node) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	p_out("Lookup on %q from %q\n", name, n.Name)
	mutex.Lock()
	n = pathmap[n.Path]
	if n.isExpanded == false {
		n.kids = make(map[string]*Node)
		expand(n)
	}
	if node, ok := n.kids[name]; ok {
		mutex.Unlock()
		return pathmap[node.Path], nil
	}
	mutex.Unlock()
	return nil, fuse.ENOENT
}

func (n *Node) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	p_out("readdir %q\n", n.Name)
	mutex.Lock()
	n = pathmap[n.Path]
	dirs := make([]fuse.Dirent, 0, 10)
	if n.isExpanded == false {
		n.kids = make(map[string]*Node)
		expand(n)
	}
	if len(n.kids) != 0 {
		for k, v := range n.kids {
			v = pathmap[v.Path]
			dirs = append(dirs, fuse.Dirent{Inode: v.Attrs.Inode, Name: k, Type: v.fuseType()})
		}
	}
	pathmap[n.Path] = n
	mutex.Unlock()
	return dirs, nil
}

// must be defined or editing w/ vi or emacs fails. Doesn't have to do anything
func (n *Node) Fsync(req *fuse.FsyncRequest, intr fs.Intr) fuse.Error {
	p_out("FSYNC\n")
	return nil
}

func (p *Node) Create(req *fuse.CreateRequest, resp *fuse.CreateResponse, intr fs.Intr) (fs.Node, fs.Handle, fuse.Error) {
	mutex.Lock()
	p = pathmap[p.Path]
	p_out("Create [%s]: (from %q) %q, mode %o\n", req, p.Name, req.Name, req.Mode)
	n := new(Node)
	n.init(req.Name, req.Mode)
	p.kids[req.Name] = n
	n.parent = p
	n.Path = p.Path + "/" + req.Name
	pathmap[n.Path] = n
	pathmap[p.Path] = p
	MarkDirty(n)
	mutex.Unlock()
	return n, n, nil
}

func (n *Node) ReadAll(intr fs.Intr) ([]byte, fuse.Error) {
	mutex.Lock()
	p_out("ReadAll called on " + n.Name)
	n = pathmap[n.Path]
	marshalnd := MarshalNode(n)
	hashnd := dfs.GetHash([]byte(marshalnd))
	toFetch := false
	if len(n.DataBlocks) > 0 {
		for i := range n.DataBlocks {
			arr := dfs.Get([]byte(n.DataBlocks[i]))
			if arr == nil {
				toFetch = true
				break
			}
		}
		if toFetch {
			fmt.Println("Inside Demand fetch")
			req := new(DataMessageReq)
			req.From = name
			req.DataReq = true
			req.NodeHash = hashnd
			fmt.Println("Requesting Data for "+n.Name+"...")
			destInfo := config[destmap[n.Path]]
			if destInfo == nil {
				destInfo = config[curSource]
			}
			port,_ := strconv.Atoi(destInfo[5])
			portNo := strconv.Itoa(port+1)
			demandClient,_ := zmq.NewSocket(zmq.REQ)
			defer demandClient.Close()
			demandClient.Connect("tcp://"+destInfo[4]+":"+portNo)
			fmt.Println("Sent Request to "+destInfo[0])
			reqToSend,_:= json.MarshalIndent(req, "", "  ")
			demandClient.Send(string(reqToSend),0)
			repData,_ := demandClient.Recv(0)
			
			var msgRep DataMessageRep
			j := json.Unmarshal([]byte(repData), &msgRep)
			if j == nil {
			}
			for key,val := range msgRep.DataMap {
				dfs.Put([]byte(key),val)
			}
			n.data = make([]byte, 0, n.Attrs.Size)
			n.DataBlocks = nil
			for i := range msgRep.DataBlocks {
				dbkey := []byte(msgRep.DataBlocks[i])
				blockdata := dfs.Get(dbkey)
				for j := range blockdata {
					n.data = append(n.data,blockdata[j])
				}
				n.DataBlocks = append(n.DataBlocks,msgRep.DataBlocks[i])
			}
		} else {
			for i := range n.DataBlocks {
				dbkey := []byte(n.DataBlocks[i])
				blockdata := dfs.Get(dbkey)
				for j := range blockdata {
					n.data = append(n.data,blockdata[j])
				}
			}
		} 
		//n.Attrs.Size = uint64(len(n.data))
	}
	pathmap[n.Path] = n
	mutex.Unlock()
	return n.data, nil
}

func (n *Node) Write(req *fuse.WriteRequest, resp *fuse.WriteResponse, intr fs.Intr) fuse.Error {
	mutex.Lock()
	p_out("Write Request on " + n.Name + " is called")
	n = pathmap[n.Path]
	olen := uint64(len(n.data))
	wlen := uint64(len(req.Data))
	offset := uint64(req.Offset)
	//offset := uint64(len(n.data))
	limit := offset + wlen
	if olen != n.Attrs.Size {
		p_out("BAD SIZE MATCH %d %d\n", olen, n.Attrs.Size)
	}
	p_out("WRITING [%v] to %q, %d bytes, offset %d, oldlen %d\n",req, n.Name, wlen, offset, olen)
	if limit > olen {
		b := make([]byte, limit)
		var tocopy uint64
		if offset < olen {
			tocopy = offset
		} else {
			tocopy = olen
		}
		copy(b[0:tocopy], n.data[0:tocopy])
		n.data = b
		n.Attrs.Size = limit
	}
	//n.Attrs.Mtime = time.Now()
	copy(n.data[offset:limit], req.Data[:])
	resp.Size = int(wlen)
	n.dirty = true
	pathmap[n.Path] = n
	MarkDirty(n)
	mutex.Unlock()
	return nil
}

func (n *Node) Rename(req *fuse.RenameRequest, newDir fs.Node, intr fs.Intr) fuse.Error {
	mutex.Lock()
	p_out("Rename request from " + req.OldName + " to " + req.NewName)
	n = pathmap[n.Path]
	x := n.kids[req.OldName]
	//if rename request for non existing node arrives, return error
	if x == nil {
		return fuse.EIO
	}
	//making name adjustments in the map and deleting entry for previous node
	x.Name = req.NewName
	x.Path = n.Path + "/" + x.Name
	n.kids[req.NewName] = x
	delete(n.kids, req.OldName)
	delete(n.ChildVids, req.OldName)
	MarkDirty(x)
	pathmap[x.Path] = x
	pathmap[n.Path] = n
	mutex.Unlock()
	return nil
}

//I could not figure out what could be the purpose of flush function, things worked fine without implementing it
func (n *Node) Flush(req *fuse.FlushRequest, intr fs.Intr) fuse.Error {
	return nil
}

func (n *Node) Remove(req *fuse.RemoveRequest, intr fs.Intr) fuse.Error {

	mutex.Lock()
	var tmpNode *Node
	n = pathmap[n.Path]
	tmpNode = n.kids[req.Name]
	//If remove request for non-existing node comes, return error
	if tmpNode == nil {
		return fuse.ENOENT
	}
	//Remove all the entries from the map associated to the node to be removed
	delete(n.kids, req.Name)
	delete(n.ChildVids, req.Name)
	MarkDirty(n)
	pathmap[n.Path] = n
	mutex.Unlock()
	return nil
}

func SaveChunks(n *Node) {
	batch := new(leveldb.Batch)
	data := n.data
	var sigs []string
	for off, l:= 0,0; off < len(data); off += l {
		l = dfs.Rkchunk(data[off:])
		p_out("chunk off %d, len %d]\n", off, l)
		sig := dfs.GetHash(data[off:off+l])
		sigs = append(sigs, sig)
		batch.Put([]byte(sig), data[off:off+l])
	}
	dfs.WriteBatch(batch)
	n.DataBlocks = sigs
}

func copyNode(src, dest *Node) {
	dest.Name = src.Name
	dest.Attrs = src.Attrs
	dest.ChildVids = src.ChildVids
	dest.Path = src.Path
	dest.DataBlocks = src.DataBlocks
	//dest.Author = src.Author
}


func Rec(n *Node) string {
	if len(n.kids) == 0 {
		y := new(Node)
		if len(n.data) > 0 {
			SaveChunks(n)
		}
		copyNode(n,y)
		//y.FlushTime = time.Now()
		marshalnd := MarshalNode(y)
		hashnd := dfs.GetHash([]byte(marshalnd))
		//n.Author = name
		//n.PreVersion = hashnd
		AddToStore(y)
		nodes[hashnd] = y
		n.metaDirty = false
		return hashnd
	} else {
		for k, v := range n.kids {
			if i := strings.Index(v.Name, "@");i >= 0 {
				continue
			}
			if v.metaDirty == true {
				n.ChildVids[k] = Rec(v)
			}
		}
		k := new(Node)
		if len(n.data) > 0 {
			SaveChunks(n)
		}
		copyNode(n,k)
		marshalnd := MarshalNode(k)
		hashnd := dfs.GetHash([]byte(marshalnd))
		AddToStore(k)
		nodes[hashnd] = k
		n.metaDirty = false
		return hashnd
	}
}

//marks parents till root as dirty
func MarkDirty(node *Node) {

	currNode := node
	for currNode != nil {
		currNode.metaDirty = true
		currNode = currNode.parent
	}
}


func (p *Node) Mkdir(req *fuse.MkdirRequest, intr fs.Intr) (fs.Node, fuse.Error) {
	mutex.Lock()
	d := new(Node)
	d.init(req.Name, os.ModeDir|0755)
	p.kids[req.Name] = d
	d.parent = p
	d.Path = p.Path + "/" + req.Name
	pathmap[d.Path] = d
	pathmap[p.Path] = p
	MarkDirty(d)
	mutex.Unlock()
	return d, nil
}

//helper functions for chunking, hash, marshal, etc.
//========================================================================================

// marshals a node using json marshaling
func MarshalNode(n *Node) string {
	j, err := json.MarshalIndent(n, "", "  ")
	if err != nil {
		return ""
	}
	return string(j)
}

// unmarshals a node
func UnMarshal(n []byte) *Node {
	var nd *Node
	j := json.Unmarshal(n, &nd)
	if j == nil {
	}
	return nd
}

func AddToStore(n *Node) {
	marshalnd := MarshalNode(n)
	hashnd := dfs.GetHash([]byte(marshalnd))
	dfs.Put([]byte(hashnd), []byte(marshalnd))
}

// helper to add node to key value store

func UpdateDB() {
	for true {
		mutex.Lock()
		if root.metaDirty == true {
			AsyncUpdate()
			msgToSend := new(Message)
			msgToSend.From = name
			msgToSend.Nodes = nodes
			msgToSend.Root = root
			j, _ := json.MarshalIndent(msgToSend, "", "  ")
			for k := range nodes {
    			delete(nodes, k)
			}
			if publisher == nil {
				fmt.Println("nil publisher")
			}
			publisher.Send(string(j),0)
		}
		mutex.Unlock()
		time.Sleep(5 * time.Second)
	}
}

func AsyncUpdate() {
	head := Rec(root)
	raftMsg := new(raft.RaftMessage)
	var raftRep raft.RaftMessage
	marshalnd := MarshalNode(root)
	hashNd := dfs.GetHash([]byte(marshalnd))
	dfs.Put([]byte("head"), []byte(head))
	fmt.Println("Sending to raft server...")
	entry := new(raft.Entry)
	entry.Value = hashNd
	raftMsg.Entries = append(raftMsg.Entries,entry)
	raftMsg.IValue = 7
	j, _ := json.MarshalIndent(raftMsg, "", "  ")
	for _,v := range config {
		reqSocket,_ := zmq.NewSocket(zmq.REQ)
		reqSocket.SetRcvtimeo(500*time.Millisecond)
		host := v[4]
		port,_ := strconv.Atoi(v[5])
		port += 7
		reqSocket.Connect("tcp://"+host+":"+strconv.Itoa(port))
		reqSocket.Send(string(j),0)
		rep,_ := reqSocket.Recv(0) 
		k := json.Unmarshal([]byte(rep),&raftRep)
		if k == nil {
		}
		if raftRep.Success == true {
			break
		}
	}
	/*
	if leaderpid == -1 {
		reqSocket,_ := zmq.NewSocket(zmq.REQ)
		defer reqSocket.Close()
		info := config[name]
		host := info[4]
		port,_ := strconv.Atoi(info[5])
		port += 7
		reqSocket.Connect("tcp://"+host+":"+strconv.Itoa(port))
		reqSocket.Send(string(j),0)
		rep,_ := reqSocket.Recv(0)
		fmt.Println("Rep Recvd")
		k := json.Unmarshal([]byte(rep),&raftRep)
		if k == nil {
		}
		leaderpid = raftRep.IValue
		if raftRep.Success == false {
			fmt.Println("Retrying..")
			info = pidmap[raftRep.IValue]
			host = info[4]
			port,_ = strconv.Atoi(info[5])
			port += 7
			reqSocket,_ = zmq.NewSocket(zmq.REQ)
			reqSocket.Connect("tcp://"+host+":"+strconv.Itoa(port))
			reqSocket.Send(string(j),0)
			rep,_ = reqSocket.Recv(0)
			if rep != "" {
				fmt.Println("Success")
			}
		}
	} else {
		fmt.Println("Retrying..")
		info := pidmap[leaderpid]
		host := info[4]
		port,_ := strconv.Atoi(info[5])
		port += 7
		reqSocket,_ := zmq.NewSocket(zmq.REQ)
		reqSocket.Connect("tcp://"+host+":"+strconv.Itoa(port))
		reqSocket.Send(string(j),0)
		rep,_ := reqSocket.Recv(0)
		if rep != "" {
			fmt.Println("Success")
		}
	}*/
}

//creates directory structure from root node 
func expand(root *Node) {
	fmt.Println("Expanding node "+root.Name)
	for k, v := range root.ChildVids {
		//fmt.Println(k + " " + v)
		var node *Node
		node = UnMarshal(dfs.Get([]byte(v)))
		if node == nil {
			req := new(DataMessageReq)
			req.From = name
			req.NodeHash = v
			req.DataReq = false
			destInfo := config[destmap[root.Path]]
			port,_ := strconv.Atoi(destInfo[5])
			portNo := strconv.Itoa(port+1)
			demandClient,_ := zmq.NewSocket(zmq.REQ)
			defer demandClient.Close()
			demandClient.Connect("tcp://"+destInfo[4]+":"+portNo)
			fmt.Println("Sent Request to "+destInfo[0]+" for node metadata...")
			reqToSend,_:= json.MarshalIndent(req, "", "  ")
			demandClient.Send(string(reqToSend),0)
			repData,_ := demandClient.Recv(0)
			var msgRep DataMessageRep
			j := json.Unmarshal([]byte(repData), &msgRep)
			if j == nil {
			}
			node = msgRep.Node
			AddToStore(node)
		}
		
		if node != nil {
			node.isExpanded = false
			node.parent = root
			node.kids = make(map[string]*Node)
			root.kids[k] = node
			pathmap[root.kids[k].Path] = root.kids[k]
		}
	}
	root.isExpanded = true
}

func exists(path string) (bool, error) {
    _, err := os.Stat(path)
    if err == nil { return true, nil }
    if os.IsNotExist(err) { return false, nil }
    return false, err
}

func receiveUpdates() {
	for {
		msg,_ := subscriber.Recv(0)
		var extMsg *Message
		j := json.Unmarshal([]byte(msg), &extMsg)
		if j == nil {
		}
		
		if extMsg != nil {
			newNodes := extMsg.Nodes
			mutex.Lock()
			//root = new(Node)
			curSource= extMsg.From
			copyNode(extMsg.Root,root)
			marshalnd := MarshalNode(root)
			hashnd := dfs.GetHash([]byte(marshalnd))
			applyUpdates(root, newNodes)
			AddToStore(extMsg.Root)
			pathmap[root.Path] = root
			destmap[root.Path] = curSource 
			dfs.Put([]byte("head"), []byte(hashnd))
			root.isExpanded = false
			//root.metaDirty = true
			mutex.Unlock()
		}
	}
}

func applyUpdates(nroot *Node, nds map[string]*Node) {
	
	if nroot.kids == nil {
		nroot.kids = make(map[string]*Node)
	}
	for key, _ := range nroot.kids {
		delete(nroot.kids, key)
	} 
	for k,v := range nroot.ChildVids {
		if nd, ok := nds[v]; ok {
			if _,ok := nroot.kids[k];ok {
				copyNode(nd, nroot.kids[k])
				nroot.kids[k].isExpanded = false
				pathmap[nroot.kids[k].Path] = nroot.kids[k]
			} else {
				nroot.kids[k] = new(Node)
				copyNode(nd,nroot.kids[k])
				nroot.kids[k].isExpanded = false
				pathmap[nd.Path] = nd
			}
			destmap[nd.Path] = curSource
			nroot.kids[k].parent = nroot
			applyUpdates(nroot.kids[k], nds)
			AddToStore(nd)
		} 
	}
	nroot.isExpanded = true
}

func startDemandServer() {

	for {
		msg, _ := demandServer.Recv(0)
		mutex.Lock()
		var  req DataMessageReq
		j := json.Unmarshal([]byte(msg), &req)
		if j == nil{
		}
		if req.DataReq == true {
			fmt.Println("Received Req from " + req.From+ " for data")
			dataMap := make(map[string][]byte)
			nd := UnMarshal(dfs.Get([]byte(req.NodeHash)))
			for i := range nd.DataBlocks {
				data := dfs.Get([]byte(nd.DataBlocks[i]))
				dataMap[nd.DataBlocks[i]] = data
			}		
			rep := new(DataMessageRep)
			rep.From = name
			rep.NodeHash = req.NodeHash
			rep.DataMap = dataMap
			rep.DataBlocks = nd.DataBlocks
			repMsg,_:= json.MarshalIndent(rep, "", "  ")
			demandServer.Send(string(repMsg),0)
		} else {
			var  req DataMessageReq
			j := json.Unmarshal([]byte(msg), &req)
			if j == nil{
			}
			fmt.Println("Received Req from " + req.From+ " for node metadata")
			d := UnMarshal(dfs.Get([]byte(req.NodeHash)))
			rep := new(DataMessageRep)
			rep.From = name
			rep.Node = d
			repMsg,_:= json.MarshalIndent(rep, "", "  ")
			demandServer.Send(string(repMsg),0)
		}
		mutex.Unlock()
	}
	
}


//=============================================================================

/*var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}*/

func main() {
	//flag.Usage = Usage

	b = 0
	p_out("main\n")
	subscriber, _ = zmq.NewSocket(zmq.SUB)
	publisher, _ = zmq.NewSocket(zmq.PUB)
	demandServer, _ = zmq.NewSocket(zmq.REP)
	
 	defer publisher.Close()
	defer subscriber.Close()
	defer demandServer.Close()
	

	//start asynchronous write
	
	/*if flag.NArg() != 1 {
		Usage()
		os.Exit(2)
	}*/
	
	debugPtr := flag.Bool("debug", false, "print lots of stuff")
	namePtr := flag.String("name", "auto", "a string")
    newfsPtr := flag.Bool("newfs", false, "a bool")
    flag.Parse()
    debug = *debugPtr
    name = *namePtr
    inputFile, _ := os.Open("config")
    defer inputFile.Close()
    hostName, _ := os.Hostname()
    addr,_ := net.LookupHost(hostName)
    fmt.Println(addr)
 
	scanner := bufio.NewScanner(inputFile)
	config = make(map[string][]string)
	if config == nil {
	}
	for scanner.Scan() {
		configLine := scanner.Text()
		if configLine[0] == '#' {
			continue
		}
		attrs := strings.Split(configLine, ",")
		config[attrs[0]] = attrs
		pid,_ := strconv.Atoi(attrs[1])
		pidmap[pid] = attrs
	}
	if *namePtr == "auto" {
		for _,v := range config {
			if v[4] == addr[0] {
				*namePtr = v[0]
				name = v[0]
				break
			}
		}
	}
	for k,v := range config {
		if v[0] == *namePtr {
			ret,_ := exists(v[2]) 
			if ret == false {
				os.Mkdir(v[2], os.ModeDir|0755)
			}
			mountPoint = v[2]
			ret1,_ := exists(v[3])
			if *newfsPtr == true {
				os.RemoveAll(v[3])	
			}
			if ret1 == false {
				os.Mkdir(v[3], os.ModeDir|0755)
			}
			dbRoot = v[3]
			dfs.SetDBRoot(dbRoot)
			publisher.Bind("tcp://"+v[4]+":"+v[5])
			port,_ := strconv.Atoi(v[5])
			portNo := strconv.Itoa(port+1)
			demandServer.Bind("tcp://"+v[4]+":"+portNo)
			fmt.Println("HOst: " + v[4])
			fmt.Println("Port: " + portNo)
			fmt.Println("publisher started")
		} else {
			
			subscriber.Connect("tcp://"+v[4]+":"+v[5])
			subscriber.SetSubscribe("")
			fmt.Println("Connected to "+ k)
		}
	}
	
	rootid := dfs.Get([]byte("head"))
	if rootid == nil {
		root = new(Node)
		root.init("root", os.ModeDir|0755)
		root.parent = nil
		//root.metaDirty = true
		marshalnd := MarshalNode(root)
		root.Path = "."
		hashnd := dfs.GetHash([]byte(marshalnd))
		dfs.Put([]byte("head"), []byte(hashnd))
		AddToStore(root)
		nextInd = 1
		vidInd = 1
	} else {
		root = UnMarshal(dfs.Get(rootid))
		fmt.Println(root)
		root.kids = make(map[string]*Node)
		//root.metaDirty = true
		root.parent = nil
		nextInd = root.Attrs.Inode
		root.Path = "."
		expand(root)
	}
	pathmap[root.Path] = root
	nodeMap[uint64(root.Attrs.Inode)] = root
	p_out("root inode %d", int(root.Attrs.Inode))

	go UpdateDB()
	go receiveUpdates()
	go startDemandServer()

	fuse.Unmount(mountPoint) //!!
	c, err := fuse.Mount(mountPoint)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	err = fs.Serve(c, FS{})
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}
