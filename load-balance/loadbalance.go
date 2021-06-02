package iloadbalance

import "load-balance/model"

type LoadBalancer interface {
	Select([]*model.ServerInstance) *model.ServerInstance
}
