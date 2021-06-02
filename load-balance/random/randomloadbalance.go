package random

import (
	"load-balance/iloadbalance"
	"math/rand"
)

type LoadBalance struct{}

func (l LoadBalance) Select(instances []*iloadbalance.ServerInstance) *iloadbalance.ServerInstance {
	if len(instances) == 0 {
		return nil
	}
	index := rand.Intn(len(instances))
	return instances[index]
}
