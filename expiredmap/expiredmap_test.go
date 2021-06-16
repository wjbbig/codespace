package expiredmap

import (
	"testing"
	"time"
)

func TestExpiredMap(t *testing.T) {
	expiredMap := NewExpiredMap()
	expiredMap.Set("key", "value", 5*time.Second)
	value := expiredMap.Get("key")
	t.Log(value)
	time.Sleep(time.Second * 5)
	value = expiredMap.Get("key")
	t.Log(value)
}
