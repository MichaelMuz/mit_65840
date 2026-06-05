package raft

import (
	"log"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...any) {
	if Debug {
		log.Printf(format, a...)
	}
}
