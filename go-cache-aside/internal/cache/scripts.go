package cache

import (
	_ "embed"

	"github.com/redis/go-redis/v9"
)

//go:embed scripts/release_lock.lua
var releaseLockLua string

var releaseLockScript = redis.NewScript(releaseLockLua)
