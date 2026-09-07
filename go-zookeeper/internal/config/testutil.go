package config

import "sync"

// ResetForTest clears the config singleton between tests.
func ResetForTest() {
	loadOnce = sync.Once{}
	instance = Config{}
	loadErr = nil
}
