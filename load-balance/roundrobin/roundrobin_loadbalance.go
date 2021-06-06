package roundrobin

import "load-balance/model"

type LoadBalance struct {
	serverPool []*model.ServerInstance
	index      uint64
}

func New(instances []*model.ServerInstance) *LoadBalance {
	return nil
}
