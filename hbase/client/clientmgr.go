package client

import "github.com/tsuna/gohbase"
import "sync"

var (
	hbaseMgr = NewGoHBaseClientManager()
)

type GoHBaseClientManager struct {
	m           sync.RWMutex
	clientCache map[string]gohbase.Client
}

func NewGoHBaseClientManager() *GoHBaseClientManager {
	var mlock sync.RWMutex
	cache := make(map[string]gohbase.Client)
	return &GoHBaseClientManager{
		m:           mlock,
		clientCache: cache,
	}
}

// CloseAll need be call when app finished, but not when tableop close
func (gcm *GoHBaseClientManager) CloseAll() {
	gcm.m.Lock()
	defer gcm.m.Unlock()

	for _, client := range gcm.clientCache {
		client.Close()
	}
}

func (gcm *GoHBaseClientManager) DelClient(zks string) {
	gcm.m.Lock()
	defer gcm.m.Unlock()

	delete(gcm.clientCache, zks)
}

func (gcm *GoHBaseClientManager) GetClient(zks string) gohbase.Client {
	var client gohbase.Client

	gcm.m.RLock()
	if client, ok := gcm.clientCache[zks]; ok {
		gcm.m.RUnlock()
		return client
	}
	gcm.m.RUnlock()

	gcm.m.Lock()
	defer gcm.m.Unlock()
	if client, ok := gcm.clientCache[zks]; ok {
		return client
	}
	client = gohbase.NewClient(zks)
	gcm.clientCache[zks] = client

	return client
}
