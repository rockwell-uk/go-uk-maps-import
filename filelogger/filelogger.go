package filelogger

import (
	"fmt"
	"io"
	"log"
)

type LogLine struct {
	File io.Writer
	Line string
}

var (
	done chan struct{}
	logs chan LogLine
)

func Start() {
	log.SetFlags(0)

	done = make(chan struct{})
	logs = make(chan LogLine, 1000)

	go monitorLoop()
}

func Stop() {
	close(logs)
	<-done
}

func Log(l LogLine) {
	logs <- l
}

func monitorLoop() {
	for l := range logs {
		if _, err := io.WriteString(l.File, fmt.Sprintln(l.Line)); err != nil {
			panic(err)
		}
	}
	close(done)
}
