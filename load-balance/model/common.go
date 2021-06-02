package model

import (
	"net/http/httputil"
	"net/url"
	"sync"
)

type ServerInstance struct {
	URL   *url.URL
	mutx  sync.RWMutex
	alive bool
	proxy *httputil.ReverseProxy
}

func NewServerInstance(urlStr string) (*ServerInstance, error) {
	target, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	return &ServerInstance{
		URL:   target,
		alive: true,
		proxy: proxy,
	}, nil
}

func (si *ServerInstance) IsAlive() bool {
	si.mutx.RLock()
	defer si.mutx.RUnlock()
	return si.alive
}

func (si *ServerInstance) SetAlive(alive bool) {
	si.mutx.Lock()
	defer si.mutx.Unlock()
	si.alive = alive
}
