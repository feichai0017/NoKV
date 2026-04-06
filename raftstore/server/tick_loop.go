package server

import "time"

func (n *Node) startRaftTickLoop(interval time.Duration) {
	if n == nil || interval <= 0 {
		return
	}
	if n.store == nil {
		return
	}
	router := n.store.Router()
	if router == nil {
		return
	}
	if n.tickStop != nil {
		return
	}
	n.tickEvery = interval
	n.tickStop = make(chan struct{})
	n.tickWG.Go(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = router.BroadcastTick()
			case <-n.tickStop:
				return
			}
		}
	})
}
