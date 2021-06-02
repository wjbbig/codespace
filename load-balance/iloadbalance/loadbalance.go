package iloadbalance

type ServerInstance struct {
	Host string
	Port string
}

type LoadBalancer interface {
	Select([]*ServerInstance) *ServerInstance
}
