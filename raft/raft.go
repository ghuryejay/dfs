package main

import (
	"flag"
	"dss/p5jghurye/raft/core"
)

func main(){
	
	namePtr := flag.String("name", "auto", "a string")
	linesPtr := flag.Int("lines",3,"an int")
	flag.Parse()
	name := *namePtr
	lines := *linesPtr
	/*
	inputFile, _ := os.Open("config")
    defer inputFile.Close()
    scanner := bufio.NewScanner(inputFile)
	config := make(map[string][]string)
	cnt := 0
	for scanner.Scan() {
		if cnt >= 3 {
			break
		}
		configLine := scanner.Text()
		attrs := strings.Split(configLine, ",")
		config[attrs[0]] = attrs
		cnt++
	}
	fmt.Println(config)
	info := config[name]
	fmt.Println(info)
	server1 := new(myraft.Server)
	var names []string
	for k,_ := range config {
		names = append(names,k)
	}
	pid,_ := strconv.Atoi(info[1])
	*/
	server1 := new(raft.Server)
	server1.NewServer(name,lines)
	server1.Start()
	for {
	}
}
