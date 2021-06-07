package main

import "load-balance/model"

type LoadBalancer interface {
	Select() *model.ServerInstance
}
