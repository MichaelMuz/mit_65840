package raft

import (
	"log"
	"sync"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...any) {
	sync.OnceFunc(func() {
		log.SetFlags(log.Lmicroseconds | log.Lshortfile)
	})

	if Debug {
		log.Printf(format, a...)
	}
}
