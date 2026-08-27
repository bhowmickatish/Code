package cache

import (
	_ "embed"

	"github.com/redis/go-redis/v9"
)

//go:embed scripts/release_lock.lua
var releaseLockLua string

//go:embed scripts/set_cache.lua
var setCacheLua string

var releaseLockScript = redis.NewScript(releaseLockLua)
var setCacheScript = redis.NewScript(setCacheLua)
