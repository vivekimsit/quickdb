package core

import "github.com/vivekimsit/quickdb/config"

func evict() {
	switch config.EvictionStrategy {
	case "simple-first":
		evictFirst()
	}
}

func evictFirst() {
	for k := range store {
		delete(store, k)
		return
	}
}
