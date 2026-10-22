"""Lua scripts: every coordination cycle is one round trip.

Upstash bills per command, so the heartbeat + lease renewal + cursor flush
must not be fanned out into N separate calls. Each script below is invoked
through ``register_script`` and executes atomically on the server.
"""

# KEYS[1] = lease key, KEYS[2] = fence counter
# ARGV[1] = token prefix "role:run_id", ARGV[2] = ttl ms
# returns {code, fence}: 1 = acquired, 2 = renewed, 0 = held by someone else
ACQUIRE = """
local cur = redis.call('GET', KEYS[1])
if not cur then
  local f = redis.call('INCR', KEYS[2])
  redis.call('SET', KEYS[1], ARGV[1] .. ':' .. f, 'PX', ARGV[2])
  return {1, f}
end
if string.sub(cur, 1, #ARGV[1] + 1) == ARGV[1] .. ':' then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  local f = tonumber(string.match(cur, ':(%d+)$'))
  return {2, f}
end
return {0, 0}
"""

# KEYS[1] = heartbeat key, KEYS[2..] = lease keys
# ARGV[1] = hb value, ARGV[2] = hb ttl ms, ARGV[3] = token prefix, ARGV[4] = lease ttl ms
# returns {renewed_count, {lost keys}}
BEAT = """
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
local renewed = 0
local lost = {}
for i = 2, #KEYS do
  local cur = redis.call('GET', KEYS[i])
  if cur and string.sub(cur, 1, #ARGV[3] + 1) == ARGV[3] .. ':' then
    redis.call('PEXPIRE', KEYS[i], ARGV[4])
    renewed = renewed + 1
  else
    table.insert(lost, KEYS[i])
  end
end
return {renewed, lost}
"""

# KEYS = [cursor1, fence1, cursor2, fence2, ...]
# ARGV = [value1, fence1, value2, fence2, ...]
# Monotonic: never move a cursor backwards. Fenced: a zombie holder whose
# fence is older than the stored one cannot advance it.
CURSOR_BATCH = """
local n = 0
for i = 1, #KEYS, 2 do
  local idx = i
  local v = tonumber(ARGV[idx])
  local fc = tonumber(ARGV[idx + 1])
  local cf = tonumber(redis.call('GET', KEYS[i + 1]) or '0')
  if v and fc and fc >= cf then
    local cur = tonumber(redis.call('GET', KEYS[i]) or '0')
    if v > cur then
      redis.call('SET', KEYS[i], v)
      redis.call('SET', KEYS[i + 1], fc)
      n = n + 1
    end
  end
end
return n
"""

# Compare-and-delete: never delete a lease that was already taken over.
RELEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""
