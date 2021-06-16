package expiredmap

import (
	"context"
	"sync"
	"time"
)

type Value struct {
	value       interface{}
	expiredTime time.Time
}

type ExpiredMap struct {
	mutex  sync.RWMutex
	data   map[string]*Value
	cancel context.CancelFunc
}

func NewExpiredMap() *ExpiredMap {
	m := &ExpiredMap{
		data: make(map[string]*Value),
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	m.cancel = cancelFunc
	go m.clean(ctx)
	return m
}

// Set set key,value of map,
func (m *ExpiredMap) Set(key string, value interface{}, duration time.Duration) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.data[key] = &Value{
		value:       value,
		expiredTime: time.Now().Add(duration),
	}
}

func (m *ExpiredMap) Get(key string) interface{} {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	value, ok := m.data[key]
	if !ok {
		return nil
	}

	return value.value
}

// clean scans for expired kv and deletes it.
func (m *ExpiredMap) clean(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			for key, value := range m.data {

				if time.Now().Equal(value.expiredTime) || time.Now().After(value.expiredTime) {
					m.mutex.Lock()
					delete(m.data, key)
					m.mutex.Unlock()
				}
			}
		}
	}
}
