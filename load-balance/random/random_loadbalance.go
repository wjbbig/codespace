package random

import (
	"load-balance/model"
	"math/rand"
)

type LoadBalance struct {
	instances []*model.ServerInstance
}

func (l LoadBalance) Select() *model.ServerInstance {
	if len(l.instances) == 0 {
		return nil
	}
	index := rand.Intn(len(l.instances))
	return l.instances[index]
}
