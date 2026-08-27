-- Atomic SET data key + LPUSH notify (same hash slot required).
-- KEYS[1] = data key, KEYS[2] = notify key
-- ARGV[1] = value, ARGV[2] = TTL seconds
redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
redis.call("LPUSH", KEYS[2], "1")
return 1
