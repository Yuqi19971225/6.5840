package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	key    string
	selfId string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:     ck,
		key:    l,
		selfId: kvtest.RandValue(8),
	}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.key)
		if err == rpc.ErrNoKey {
			if lk.ck.Put(lk.key, lk.selfId, 0) == rpc.OK {
				return
			}
			continue
		}
		if err == rpc.OK {
			if value == lk.selfId {
				return
			}
			if value != "" {
				continue
			}
			if lk.ck.Put(lk.key, lk.selfId, version) == rpc.OK {
				return
			}
		}
	}

}

func (lk *Lock) Release() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.key)
		if err == rpc.ErrNoKey {
			return
		}
		if err == rpc.OK && value == lk.selfId {
			err := lk.ck.Put(lk.key, "", version)
			if err == rpc.OK {
				return
			}
		} else {
			return
		}
	}
}
