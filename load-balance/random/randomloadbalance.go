package random

import (
	"load-balance/model"
	"math/rand"
)

type LoadBalance struct{}

func (l LoadBalance) Select(instances []*model.ServerInstance) *model.ServerInstance {
	if len(instances) == 0 {
		return nil
	}
	index := rand.Intn(len(instances))
	return instances[index]
}
