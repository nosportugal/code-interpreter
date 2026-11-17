import { existsSync, readFileSync } from 'fs';
import IORedis, { Cluster } from 'ioredis';
import type {
  Redis,
  RedisOptions,
  ClusterOptions,
  CommonRedisOptions,
} from 'ioredis';
import { redisKeepAliveOptions } from './redis-options';
import logger from './logger';

export type RedisClient = Redis | Cluster;

const MAX_CLUSTER_RECONNECT_ATTEMPTS = 5;

/**
 * Parse REDIS_HOST into an array of {host, port} startup nodes.
 * Accepts comma-separated entries in "host:port", "[ipv6]:port", or plain
 * "host" format. Unbracketed IPv6 addresses are treated as plain hosts.
 * Falls back to REDIS_PORT (default 6379) when no port is embedded.
 */
export function parseRedisNodes(): Array<{ host: string; port: number }> {
  const defaultPort = Number(process.env.REDIS_PORT) || 6379;
  const raw = process.env.REDIS_HOST ?? 'redis';
  return raw.split(',').map(entry => {
    const trimmed = entry.trim();
    if (trimmed.startsWith('[')) {
      const closingBracket = trimmed.indexOf(']');
      if (closingBracket > 0) {
        const host = trimmed.slice(1, closingBracket);
        const portPart = trimmed.slice(closingBracket + 1);
        if (portPart.startsWith(':')) {
          const port = Number(portPart.slice(1));
          if (Number.isInteger(port) && port > 0) {
            return { host, port };
          }
        }
        return { host, port: defaultPort };
      }
    }

    if (trimmed.indexOf(':') === trimmed.lastIndexOf(':')) {
      const colonIdx = trimmed.indexOf(':');
      const port = Number(trimmed.slice(colonIdx + 1));
      if (colonIdx > 0 && Number.isInteger(port) && port > 0) {
        return { host: trimmed.slice(0, colonIdx), port };
      }
    }

    return { host: trimmed, port: defaultPort };
  });
}

/**
 * Returns true when Redis Cluster mode is active.
 * Cluster mode is enabled when USE_REDIS_CLUSTER=true or REDIS_HOST contains
 * a comma-separated list of nodes.
 */
export function isClusterMode(): boolean {
  return (
    process.env.USE_REDIS_CLUSTER === 'true' ||
        (process.env.REDIS_HOST ?? '').includes(',')
  );
}

/**
 * Read and return the PEM-encoded CA certificate from the path given by the
 * REDIS_CA environment variable.  Returns null when the variable is unset or
 * the file cannot be read.
 */
function readCACert(): string | null {
  const caPath = process.env.REDIS_CA;
  if (!caPath) return null;
  try {
    if (!existsSync(caPath)) {
      logger.warn(`Redis CA certificate file not found: ${caPath}`);
      return null;
    }
    return readFileSync(caPath, 'utf8');
  } catch (error) {
    logger.error(`Failed to read Redis CA certificate: ${caPath}`, {
      error,
    });
    return null;
  }
}

/**
 * Build the TLS options for an ioredis connection:
 * - REDIS_CA points to a file → use that CA (certificate fully validated).
 * - REDIS_TLS=true without a CA → disable certificate validation (backward-compatible).
 * - Neither set → no TLS.
 */
export function buildTlsOptions(): Record<string, unknown> | undefined {
  const ca = readCACert();
  if (ca) return { ca };
  if (process.env.REDIS_TLS === 'true') return { rejectUnauthorized: false };
  return undefined;
}

/**
 * The REDIS_PREFIX namespace, isolating deployments that share one Redis.
 * Returns undefined when unset or blank. Surrounding `{}` and trailing `:` are
 * tolerated and stripped; any other brace is rejected because, in cluster mode,
 * it would override the per-id hash tags every multi-key script relies on.
 */
export function redisPrefix(): string | undefined {
  const raw = process.env.REDIS_PREFIX?.trim();
  if (!raw) return undefined;
  const name = stripHashTag(raw).replace(/:+$/, '');
  if (!name) return undefined;
  if (/[{}]/.test(name)) {
    throw new Error(`REDIS_PREFIX must not contain '{' or '}': ${raw}`);
  }
  return name;
}

/**
 * ioredis `keyPrefix` applied to every non-BullMQ client: `<REDIS_PREFIX>:`,
 * or '' when REDIS_PREFIX is unset.
 */
export function redisKeyPrefix(): string {
  const name = redisPrefix();
  return name ? `${name}:` : '';
}

/**
 * Returns the BullMQ `prefix`. Cluster mode always wraps it in a hash tag so
 * all queue keys land in the same slot, defaulting to `{codeapi}`. Standalone
 * mode uses REDIS_PREFIX as-is, or undefined (BullMQ's own default) when unset.
 */
export function bullmqPrefix(): string | undefined {
  const name = redisPrefix();
  if (!isClusterMode()) return name;
  return hashTag(name ?? 'codeapi');
}

/** The ioredis `keyPrefix` a client transparently applies to command keys. */
export function clientKeyPrefix(client: RedisClient): string {
  return client.options?.keyPrefix ?? '';
}

/**
 * Prepend the client's key prefix to a name ioredis does not prefix on its own:
 * Lua ARGV that the script turns into keys, and pub/sub channels.
 */
export function withClientKeyPrefix(client: RedisClient, name: string): string {
  return `${clientKeyPrefix(client)}${name}`;
}

/**
 * Strip the client's key prefix from a raw key name returned by Redis (SCAN
 * results, or key names a Lua script stored from its already-prefixed KEYS),
 * so it can be passed back to the same client without double-prefixing.
 */
export function stripClientKeyPrefix(client: RedisClient, key: string): string {
  const prefix = clientKeyPrefix(client);
  return prefix && key.startsWith(prefix) ? key.slice(prefix.length) : key;
}

/** SCAN MATCH pattern covering the client's key prefix (glob-escaped). */
export function clientScanPattern(client: RedisClient, match: string): string {
  return `${clientKeyPrefix(client).replace(/[*?[\]\\]/g, '\\$&')}${match}`;
}

/**
 * Wrap an id in a Redis Cluster hash tag so every key built from it — no
 * matter the prefix — hashes to the same slot. Required for any multi-key
 * Lua script or MULTI/EXEC transaction that touches several keys sharing
 * this id; harmless in standalone mode. Redis Cluster hashes only the
 * substring between the first `{` and the following `}` in a key.
 */
export function hashTag(id: string): string {
  return `{${id}}`;
}

/** Reverse of `hashTag`: strips the surrounding `{}` if present, otherwise
 * returns the input unchanged. Used to recover the raw id from a key that
 * was built with `hashTag`. */
export function stripHashTag(raw: string): string {
  return raw.startsWith('{') && raw.endsWith('}') ? raw.slice(1, -1) : raw;
}

/** Default cap on keys returned by `scanKeys` in a single call, bounding
 * memory usage on a pathological keyspace. */
export const SCAN_KEYS_DEFAULT_LIMIT = 10_000;

/** Iterate matching Redis keys with SCAN instead of the blocking KEYS
 * command. Stops collecting once `limit` keys have been gathered. In
 * cluster mode, SCAN is issued against every master node individually
 * because `Cluster` has no top-level `scanStream`; each node owns a
 * disjoint set of hash slots so there are no duplicates across nodes.
 * `match` and the returned keys are relative to the client's key prefix. */
export async function scanKeys(
  client: RedisClient,
  match: string,
  count = 200,
  limit = SCAN_KEYS_DEFAULT_LIMIT
): Promise<string[]> {
  const out: string[] = [];
  const pattern = clientScanPattern(client, match);

  const collect = async (node: Redis): Promise<void> => {
    const stream = node.scanStream({ match: pattern, count });
    for await (const batch of stream as AsyncIterable<string[]>) {
      for (const key of batch) {
        out.push(stripClientKeyPrefix(client, key));
        if (out.length >= limit) {
          stream.destroy();
          logger.warn(
            'scanKeys hit limit; remaining keys deferred to next pass',
            { match, limit }
          );
          return;
        }
      }
    }
  };

  if (client instanceof Cluster) {
    for (const node of client.nodes('master')) {
      if (out.length >= limit) break;
      await collect(node);
    }
  } else {
    await collect(client);
  }

  return out;
}

type ConnectionOverrides = Partial<
    Pick<
        CommonRedisOptions,
        | 'maxRetriesPerRequest'
        | 'enableReadyCheck'
        | 'retryStrategy'
        | 'reconnectOnError'
        | 'connectTimeout'
    >
>;

/**
 * Create an ioredis client – standalone `Redis` or `Cluster` – based on the
 * current environment configuration.
 *
 * Callers supply per-client `overrides` (retry strategy, maxRetriesPerRequest,
 * enableReadyCheck, …) to preserve each component's existing connection semantics.
 *
 * In cluster mode the overrides are forwarded to `redisOptions` (applied
 * per-node).  The cluster-level reconnect loop is handled independently via
 * `clusterRetryStrategy`.
 *
 * Every key is namespaced with REDIS_PREFIX through ioredis `keyPrefix`, unless
 * `forBullmq` is set: BullMQ rejects `keyPrefix` connections and namespaces its
 * keys through its own `prefix` option (see `bullmqPrefix`).
 */
export function createRedisConnection(
  overrides: ConnectionOverrides,
  { forBullmq = false }: { forBullmq?: boolean } = {}
): RedisClient {
  const keyPrefix = forBullmq ? '' : redisKeyPrefix();
  const tls = buildTlsOptions() as RedisOptions['tls'];
  const dnsLookup: ClusterOptions['dnsLookup'] | undefined =
        process.env.REDIS_USE_ALTERNATIVE_DNS_LOOKUP === 'true'
          ? (address, callback) => callback(null, address)
          : undefined;

  const baseOptions: RedisOptions = {
    password: process.env.REDIS_PASSWORD,
    connectTimeout: 10000,
    ...redisKeepAliveOptions(),
    ...(tls !== undefined ? { tls } : {}),
    ...(dnsLookup ? { dnsLookup } : {}),
    ...overrides,
  };

  if (isClusterMode()) {
    const nodes = parseRedisNodes();
    return new Cluster(nodes, {
      ...(dnsLookup ? { dnsLookup } : {}),
      ...(keyPrefix ? { keyPrefix } : {}),
      redisOptions: baseOptions,
      clusterRetryStrategy(times) {
        if (times > MAX_CLUSTER_RECONNECT_ATTEMPTS) {
          logger.error(
            `Redis cluster giving up after ${times} reconnection attempts`
          );
          return null;
        }
        const base = Math.min(2 ** times * 100, 3000);
        const jitter = Math.floor(Math.random() * Math.min(base, 1000));
        return Math.min(base + jitter, 3000);
      },
      enableOfflineQueue: true,
    });
  }

  const [{ host, port }] = parseRedisNodes();
  return new IORedis({
    host,
    port,
    ...baseOptions,
    ...(keyPrefix ? { keyPrefix } : {}),
  });
}
