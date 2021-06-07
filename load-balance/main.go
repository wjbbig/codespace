package main

import (
	"flag"
	"fmt"
	"load-balance/model"
	"load-balance/roundrobin"
	"net/http"
	"strings"
)

var (
	port    = flag.Int("port", 8080, "proxy server port")
	servers = flag.String("servers", "", "backend server addresses")
)

type Server struct {
	port int
	lb   LoadBalancer
}

func NewServer(port int, urls ...string) *Server {
	if len(urls) == 0 {
		panic("urls can not be empty")
	}
	var instances []*model.ServerInstance
	for _, url := range urls {
		instance, err := model.NewServerInstance(url)
		if err != nil {
			panic(err)
		}
		instances = append(instances, instance)
	}
	lb := roundrobin.New(instances)
	return &Server{
		port: port,
		lb:   lb,
	}
}

func (s *Server) Run() {
	server := http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: http.HandlerFunc(s.proxy),
	}

	server.ListenAndServe()
}

func (s *Server) proxy(w http.ResponseWriter, r *http.Request) {
	instance := s.lb.Select()
	instance.Proxy.ServeHTTP(w, r)
}

func main() {
	flag.Parse()
	if len(*servers) == 0 {
		panic("servers can not be nil")
	}
	urls := strings.Split(*servers, ",")
	server := NewServer(*port, urls...)
	server.Run()
}
