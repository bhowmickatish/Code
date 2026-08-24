-- Atomic compare-and-del: only the lock holder can release.
-- Redis has no UNLOCK command; GET+DEL must run atomically via Lua.
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
