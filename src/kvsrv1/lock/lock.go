package lock

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	// "sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...any) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// seems like the point is we are given a clerk to use that will allow us to use our 'distributed' k/v server
// we need to use this to implement distributed locks. The user of this 'library' is able to acquire and release
// without really caring about the distributed details

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	name string
}

const locked = "locked"
const unlocked = "unlocked"

// var autoinc = atomic.Uint64{}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	DPrintf("Attempting make lock, lockname: %v", lockname)
	err := ck.Put(lockname, unlocked, 0)
	if err == rpc.ErrNoKey {
		log.Fatal("Impossible state, no key on init but we used 0")
	} else if err == rpc.ErrVersion {
		// Distributed lock so many clients will try to make the same one, this is ok
		DPrintf("Already initialized by another client lockname: %v", lockname)
	} else if err != rpc.OK {
		log.Fatalf("Impossible state, err: %v", err)
	}
	return &Lock{ck: ck, name: lockname}
}

func (lk *Lock) Acquire() {
	DPrintf("Attempting acquire, lockname: %v", lk.name)
	for {
		val, ver, err := lk.ck.Get(lk.name)
		if err == rpc.ErrNoKey {
			log.Fatal("Attempt to acquire non-existent lock")
		} else if err != rpc.OK {
			log.Fatalf("Impossible state, err: %v", err)
		} else if val == locked {
			DPrintf("Already aquired, sleeping. lockname: %v", lk.name)
			time.Sleep(time.Second)
			continue
		} else if val != unlocked {
			log.Fatal("Impossible state, lock is neither locked nor unlocked")
		}

		DPrintf("Unlocked, lockname: %v", lk.name)

		err = lk.ck.Put(lk.name, locked, ver)

		if err == rpc.ErrNoKey {
			log.Fatal("Impossible state, get worked but put says no key")
		} else if err == rpc.ErrVersion {
			DPrintf("Just aquired by another, sleeping. lockhame: %v", lk.name)
			time.Sleep(time.Second)
			continue
		} else if err == rpc.OK {
			DPrintf("Aquired lock: %v", lk.name)
			break
		} else {
			log.Fatalf("Impossible state, err: %v", err)
		}
	}
}

func (lk *Lock) Release() {
	DPrintf("Attempting release, lockname: %v", lk.name)

	val, ver, err := lk.ck.Get(lk.name)
	if err == rpc.ErrNoKey {
		log.Fatal("Attempt to release non-existent lock")
	} else if err != rpc.OK {
		log.Fatalf("Impossible state, err: %v", err)
	} else if val == unlocked {
		log.Fatalf("Attempt to release unlocked lock %v", lk.name)
	} else if val != locked {
		log.Fatal("Impossible state, lock is neither locked nor unlocked")
	}

	err = lk.ck.Put(lk.name, unlocked, ver)

	switch err {
	case rpc.ErrNoKey:
		log.Fatal("Impossible state, get worked but put says no key")
	case rpc.ErrVersion:
		log.Fatalf("Unauthorized lock access of %v", lk.name)
	case rpc.OK:
		DPrintf("Released lock: %v", lk.name)
	default:
		log.Fatalf("Impossible state, err: %v", err)
	}
}
