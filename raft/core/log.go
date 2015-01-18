package raft

import "fmt"
import "strconv"

type Log struct {
	Term	int
	Index	int
	Entries	[]*Entry
}

func (l *Log) GetTerm() int {
	return l.Term
}

func (l *Log) GetIndex() int {
	return l.Index
}

func (l *Log) GetLastIndex() int {
	return l.Index - 1
}

func (l *Log) GetEntry(index int) *Entry{
	return l.Entries[index]
}

func (l *Log) IsFresher(term int, index int) bool{
	if l.Term > term {
		return true
	}
	
	if l.Term < term {
		return false
	}
	
	return l.Index > index
}

func(l *Log) GetPrevLogIndex() int {
	sz := len(l.Entries)
	if sz != 0 {
		return l.Entries[sz-1].Index
	} else {
		return 0
	}
}

func(l *Log) GetPrevLogTerm() int {
	sz := len(l.Entries)
	if sz != 0 {
		return l.Entries[sz-1].Term
	} else {
		return 0
	}
}

func (l* Log) GetEntryForRequest(index int) ([]*Entry, int, int) {
	var ret []*Entry
	if index == 1 {
		return l.Entries[0:],l.Term,0
	}
	if index > len(l.Entries) {
		//fmt.Println("First case")
		return nil,-1,-1
	}
	if index < 1 {
		//fmt.Println("Second case")
		return nil,-1,-1
	}
	if index < 2 {
		//fmt.Println("THird case")
		ret = append(ret,l.Entries[index-1])
		return ret, -1, -1
	}
	//fmt.Println("nothing")
	return l.Entries[index-1:], l.Entries[index-2].Index, l.Entries[index-2].Term
}

func (l *Log) Check(prevLogIndex, prevLogTerm, index, term int) bool {
	if len(l.Entries) == 0 && index > 1 {
		return false
	}
	if len(l.Entries) > 0 && index > 1 {
		
		if index > len(l.Entries)+1 {
			//fmt.Println(index)
			//fmt.Println(len(l.Entries))
			return false
		}
		lastValidEntry := l.Entries[index-2]
		//fmt.Println(lastValidEntry)
		if lastValidEntry.Term != prevLogTerm || lastValidEntry.Index != prevLogIndex {
			return false
		} 
	}
	return true
}

func (l *Log) Append(e *Entry) bool{
	if e.Term != l.Term {
		l.Term = e.Term
	}
	length := len(l.Entries)
	if length != 0 {
		ent := l.Entries[length-1]
		if ent.Index == e.Index {
			return false
		}
	}
	if e.Index < l.Index {
		l.Entries = l.Entries[:e.Index]
	}
	l.Entries = append(l.Entries,e)
	fmt.Println("RECVD APPEND_REQ Term "+strconv.Itoa(e.Term)+" Index "+strconv.Itoa(e.Index))
	l.Index = e.Index
	return true
}
