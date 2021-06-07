package roundrobin

import (
	"load-balance/model"
	"sync/atomic"
)

type LoadBalance struct {
	serverPool   []*model.ServerInstance
	currentIndex uint64
}

func New(instances []*model.ServerInstance) *LoadBalance {

	lb := &LoadBalance{
		serverPool: instances,
	}
	atomic.StoreUint64(&lb.currentIndex, 0)
	return lb
}

func (rr *LoadBalance) Select() *model.ServerInstance {
	index := int(atomic.AddUint64(&rr.currentIndex, 1)) % len(rr.serverPool)
	return rr.serverPool[index]
}
