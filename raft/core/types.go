package raft

type Entry struct {
	Vtype int // 1 - null, 2 - group size change, 3 - client value
	Size int // only used for changing group size
	Index	int
	Term	int
	Value string // string instead of []byte for debugging and me
}

type Peer struct {
	Name		string
	NextIndex	int
}


type RaftMessage struct {
	Id				int
	Vtype 			int
	Success 		bool
	IValue			int // multipurpose integer value
	Term			int
	Index			int
	Name			string
	LeaderId		string
	LeaderPid		int
	CandidateId		string
	PrevLogIndex	int
	PrevLogTerm		int
	CommitIndex		int
	Rep_Term		int
	SizeChange		bool
	Size			int
	State			int
	IsHeartBeat		bool
	WasTermStale	bool
	IsComplete		bool
	Committed		bool
	IsCatchup		bool
	Entries			[]*Entry
}

const (
	FOLLOWER = 1
	CANDIDATE = 2
	LEADER = 3
)

const (
	_ = iota
	ENTRY_SIZE // ’Size’ has new size request
	ENTRY_CLIENT // ’Value’ has data
)

const (
	_=iota
	HANGING
	COMMITTED
)


// message types for raft internals, and communicating with client
const (
	_ = iota
	RAFT_APPEND_REQ // ’Value’ has data
	RAFT_APPEND_REP // ’Value has data
	RAFT_VOTE_REQ // ’Value has data
	RAFT_VOTE_REP // ’Value has data
	RAFT_CLIENT_SIZE_REQ // To replica, asking to change size of group
	RAFT_CLIENT_SIZE_REPLY // To client, w/ ’Success’ and ’Ivalue’ (used to hold// pid of actual leader replica.
	RAFT_CLIENT_VALUE_REQ // To replica, asking to append a value to log.
	RAFT_CLIENT_VALUE_REPLY // To client, w/ ’Success’ and ’Ivalue’ (used to hold// pid of actual leader replica.
)

