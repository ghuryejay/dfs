package dfs

import (
	"github.com/syndtr/goleveldb/leveldb"
)

const (
	HASHLEN = 32
	PRIME = 31
 	MINCHUNK = 2048
 	TARGETCHUNK = 4096
 	MAXCHUNK = 8192
)

var b, b_n, hash uint64
var saved [256]uint64

var dbRoot string

func SetDBRoot(str string){
	dbRoot = str
}

func Rkchunk(buf []byte) int {
	var hash 	uint64
	var off		int
	b = PRIME
	b_n := uint64(1)
	for i := 0; i < (HASHLEN-1); i++ {
		b_n = b_n * b
	}
	for i := 0; i < 256; i++ {
		saved[i] = uint64(uint64(i) * b_n)
	}

	for off = 0; (off < HASHLEN) && (off < len(buf)); off++ {
		hash = hash * b + uint64(buf[off])
	}

	for off < len(buf) {
		hash = (hash - uint64(saved[buf[off-HASHLEN]])) * b + uint64(buf[off])
		off++
		if ((off >= MINCHUNK) && ((hash % TARGETCHUNK) == 1)) || (off >= MAXCHUNK) {
			return off
		}
	}
	return off
}



func WriteBatch(batch *leveldb.Batch) {
	db, _ := leveldb.OpenFile(dbRoot+"/golevel.db", nil)
	defer db.Close()
	err := db.Write(batch, nil)
	if err != nil {
	}
}

func Put(key, value []byte) {
	db, err := leveldb.OpenFile(dbRoot+"/golevel.db", nil)
	defer db.Close()
	if err != nil {
	}
	err = db.Put(key, value, nil)
	if err != nil {
	}
}

func Get(key []byte) []byte {
	db, err := leveldb.OpenFile(dbRoot+"/golevel.db", nil)
	defer db.Close()
	if err != nil {
	}
	ret, err1 := db.Get(key, nil)
	if err1 != nil {
		return nil
	}
	return ret
}




