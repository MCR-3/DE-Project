"""The Redis side of a FSA run: the Authority queue its Fetchers share, and the global rate limit.

Everything the Fetchers of a run share goes through Redis (see docs/adr/0002); they never talk to
each other. Redis only coordinates: what is finished is decided from the landed files (`done.json`),
so a run's keys can be rebuilt from them at any time (that is what `prepare` does).

Keys of one run (deleted when its manifest is written, otherwise they expire after RUN_TTL):

    fsa:<run>:queue     LIST  authority ids still to fetch, largest first (popped from the left)
    fsa:<run>:leases    ZSET  authority id -> lease expiry (ms, Redis clock) of the ones being fetched
    fsa:<run>:owners    HASH  authority id -> token of the Fetcher that holds its lease
    fsa:<run>:attempts  HASH  authority id -> number of failed attempts
    fsa:<run>:failed    SET   authority ids set aside after MAX_ATTEMPTS failures
    fsa:<run>:errors    HASH  authority id -> last error message

Keys shared by every run (the FSA sees our IP address, not our runs):

    fsa:ratelimit:next  STRING  earliest time (ms, Redis clock) the next request may be sent
    fsa:cooldown        STRING  set after a 429, expires when the FSA's Retry-After has passed

Every multi-key step is a Lua script, so it is atomic, and all times come from the Redis server's
clock, so Fetchers in different containers agree on when a lease expires.
"""

import uuid

import redis

RUN_TTL_SECONDS = 7 * 24 * 3600
LEASE_MS = 120_000  # a lease not renewed for this long is taken back
MAX_ATTEMPTS = 3

_NOW = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
"""

# KEYS: queue, leases, owners. ARGV: lease ms, token. Takes back the expired leases (to the front of
# the queue, they have pages saved already), then pops the next authority and leases it.
_CLAIM = _NOW + """
for _, id in ipairs(redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now)) do
    redis.call('ZREM', KEYS[2], id)
    redis.call('HDEL', KEYS[3], id)
    redis.call('LPUSH', KEYS[1], id)
end
local id = redis.call('LPOP', KEYS[1])
if not id then
    return {false, redis.call('ZCARD', KEYS[2])}
end
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[1]), id)
redis.call('HSET', KEYS[3], id, ARGV[2])
return {id, 0}
"""

# KEYS: leases, owners. ARGV: id, token, lease ms. 1 if renewed, 0 if the lease is no longer ours.
_RENEW = _NOW + """
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('ZADD', KEYS[1], now + tonumber(ARGV[3]), ARGV[1])
return 1
"""

# KEYS: leases, owners. ARGV: id, token. Releases the lease if it is still ours (1), else 0.
_RELEASE = """
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return 1
"""

# KEYS: queue, leases, owners, attempts, failed, errors. ARGV: id, token, max attempts, error.
# -1 if the lease was no longer ours, else the number of attempts so far (set aside at the maximum).
_FAIL = """
if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[2] then return -1 end
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('HSET', KEYS[6], ARGV[1], ARGV[4])
local attempts = redis.call('HINCRBY', KEYS[4], ARGV[1], 1)
if attempts >= tonumber(ARGV[3]) then
    redis.call('SADD', KEYS[5], ARGV[1])
else
    redis.call('RPUSH', KEYS[1], ARGV[1])
end
return attempts
"""

# KEYS: queue, attempts, failed. Puts the authorities set aside back at the end of the queue, with
# their attempts reset. Returns how many.
_REQUEUE_FAILED = """
local ids = redis.call('SMEMBERS', KEYS[3])
for _, id in ipairs(ids) do
    redis.call('RPUSH', KEYS[1], id)
    redis.call('HDEL', KEYS[2], id)
end
redis.call('DEL', KEYS[3])
return #ids
"""

# KEYS: next slot, cooldown. ARGV: ms between two requests.
# Returns how long the caller must wait (ms) before sending: 0 means "send now" (slot taken).
_ACQUIRE = _NOW + """
local cooldown = redis.call('PTTL', KEYS[2])
if cooldown > 0 then return cooldown end
local slot = tonumber(redis.call('GET', KEYS[1]) or '0')
if slot > now then return slot - now end
redis.call('SET', KEYS[1], now + tonumber(ARGV[1]), 'PX', 60000)
return 0
"""

# KEYS: cooldown. ARGV: ms. Starts (or extends, never shortens) the shared cooldown.
_COOLDOWN = """
if redis.call('PTTL', KEYS[1]) < tonumber(ARGV[1]) then
    redis.call('SET', KEYS[1], '1', 'PX', ARGV[1])
end
return 1
"""


def connect(host, port, password, db=0):
    return redis.Redis(host=host, port=port, password=password or None, db=db, decode_responses=True)


class RateLimiter:
    """At most `max_rps` requests per second to the FSA, shared by every Fetcher of every run."""

    def __init__(self, client, max_rps):
        self._interval_ms = max(1, round(1000 / max_rps))
        self._acquire = client.register_script(_ACQUIRE)
        self._cooldown = client.register_script(_COOLDOWN)

    def wait_ms(self):
        """0 if a request may be sent now (and takes that slot), else how long to wait first."""
        return self._acquire(keys=["fsa:ratelimit:next", "fsa:cooldown"], args=[self._interval_ms])

    def cool_down(self, seconds):
        """Make every Fetcher pause for `seconds` (after a 429)."""
        self._cooldown(keys=["fsa:cooldown"], args=[max(1, round(seconds * 1000))])


class AuthorityQueue:
    """The Authority queue of one run, seen from one Fetcher (each instance has its own token)."""

    def __init__(self, client, run_id):
        self.client = client
        prefix = f"fsa:{run_id}:"
        self.queue, self.leases, self.owners = prefix + "queue", prefix + "leases", prefix + "owners"
        self.attempts, self.failed, self.errors = prefix + "attempts", prefix + "failed", prefix + "errors"
        self.token = uuid.uuid4().hex
        self._claim = client.register_script(_CLAIM)
        self._renew = client.register_script(_RENEW)
        self._release = client.register_script(_RELEASE)
        self._fail = client.register_script(_FAIL)
        self._requeue_failed = client.register_script(_REQUEUE_FAILED)

    def _keys(self):
        return [self.queue, self.leases, self.owners, self.attempts, self.failed, self.errors]

    def reset(self, la_ids):
        """Replace the run's queue by `la_ids` (in that order), dropping every lease, attempt and failure."""
        with self.client.pipeline() as pipe:
            pipe.delete(*self._keys())
            if la_ids:
                pipe.rpush(self.queue, *la_ids)
                pipe.expire(self.queue, RUN_TTL_SECONDS)
            pipe.execute()

    def delete(self):
        self.client.delete(*self._keys())

    def claim(self):
        """(authority id, 0) if one was leased; (None, n) if the queue is empty and n are still leased."""
        la_id, leased = self._claim(keys=[self.queue, self.leases, self.owners], args=[LEASE_MS, self.token])
        for key in self._keys():
            self.client.expire(key, RUN_TTL_SECONDS, nx=True)
        return (int(la_id) if la_id else None), leased

    def renew(self, la_id):
        """Extend our lease on la_id. False if it is no longer ours (it expired and was taken back)."""
        return bool(self._renew(keys=[self.leases, self.owners], args=[la_id, self.token, LEASE_MS]))

    def release(self, la_id):
        """The authority is done: drop its lease. False if it was no longer ours."""
        return bool(self._release(keys=[self.leases, self.owners], args=[la_id, self.token]))

    def fail(self, la_id, error):
        """Count a failed attempt; the authority goes back in the queue, or is set aside after MAX_ATTEMPTS.

        Returns the number of attempts so far, or None if the lease was no longer ours.
        """
        attempts = self._fail(
            keys=[self.queue, self.leases, self.owners, self.attempts, self.failed, self.errors],
            args=[la_id, self.token, MAX_ATTEMPTS, error[:500]],
        )
        return None if attempts < 0 else attempts

    def requeue_failed(self):
        """Give the authorities set aside another round of attempts. Returns how many."""
        return self._requeue_failed(keys=[self.queue, self.attempts, self.failed])

    def failures(self):
        """{authority id: last error} of the authorities set aside."""
        failed = self.client.smembers(self.failed)
        errors = self.client.hgetall(self.errors)
        return {int(i): errors.get(i, "") for i in failed}
