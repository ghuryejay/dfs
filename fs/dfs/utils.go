package dfs

import (
	"crypto/sha1"
	"encoding/base64"
	"time"
)

func ParseTime(s string) (time.Time, bool) {
	timeFormats := []string{"2006-1-2 15:04:05", "2006-1-2 15:04", "2006-1-2", "1-2-2006 15:04:05", 
		"1-2-2006 15:04", "1-6-2006", "2006/1/2 15:04:05", "2006/1/2 15:04", "2006/1/2", 
		"1/2/2006 15:04:05", "1/2/2006 15:04", "1/2/2006"}
	loc, _ := time.LoadLocation("Local")

	for _,v := range timeFormats {
		if tm, terr := time.ParseInLocation(v, s, loc); terr == nil {
			return tm, false
		}
	}
	return time.Time{}, true
}

//calculates encoded hash value
func GetHash(data []byte) string {
	hasher := sha1.New()
	hasher.Write(data)
	sha := base64.URLEncoding.EncodeToString(hasher.Sum(nil))
	return sha
}


func MakeTimestamp(ts time.Time) int64 {
    return ts.UnixNano() / int64(time.Millisecond)
}
