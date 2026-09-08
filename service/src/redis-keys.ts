/**
 * Optional global namespace for every Redis key CodeAPI owns.
 *
 * CodeAPI's key families (`codeapi:bridge:*`, `exec_state:*`, `tool_call:*`,
 * `session:*`, `rtsx:*`, the BullMQ prefix, …) are unqualified by default,
 * which is fine on a dedicated Redis. On a shared instance — a single GCP
 * Memorystore cluster serving CodeAPI alongside LibreChat, for example —
 * `REDIS_KEY_PREFIX` moves the whole keyspace under one namespace so the
 * tenants cannot collide and each can be swept independently.
 *
 * Applied client-side at key construction rather than through ioredis'
 * `keyPrefix`, because BullMQ rejects a connection carrying `keyPrefix` and
 * ioredis does not prefix `SCAN`/`scanStream` match patterns.
 *
 * The prefix is a pure namespace change: nothing outside CodeAPI reads these
 * keys, so it can be set, changed or removed freely. Any in-flight state
 * under the old namespace is orphaned and expires on its own TTL, so change
 * it while idle.
 */

let cachedRaw: string | undefined;
let cachedPrefix = '';

/**
 * The normalized prefix — either empty, or the configured value with exactly
 * one trailing `:`. Re-derived whenever `REDIS_KEY_PREFIX` changes so tests
 * can flip it without reloading the module.
 */
export function redisKeyPrefix(): string {
    const raw = process.env.REDIS_KEY_PREFIX ?? '';
    if (raw === cachedRaw) return cachedPrefix;
    cachedRaw = raw;
    const trimmed = raw.trim().replace(/:+$/, '');
    cachedPrefix = trimmed === '' ? '' : `${trimmed}:`;
    return cachedPrefix;
}

/**
 * Namespace a key or a `SCAN` match pattern. Safe with Redis Cluster hash
 * tags: the prefix contains no braces, so the first `{…}` in the result is
 * still the caller's tag and the slot is unchanged.
 */
export function redisKey(key: string): string {
    return `${redisKeyPrefix()}${key}`;
}
