import { describe, expect, test, afterEach } from 'bun:test';
import {
  parseRedisNodes,
  isClusterMode,
  buildTlsOptions,
  bullmqPrefix,
  redisPrefix,
  redisKeyPrefix,
  clientKeyPrefix,
  withClientKeyPrefix,
  stripClientKeyPrefix,
  clientScanPattern,
  createRedisConnection,
  type RedisClient,
} from './redis-connection';

// ──────────────────────────────────────────────────────────────────────────────
// We test the pure helper functions from redis-connection.ts without
// establishing any real network connection.  The only ioredis clients built
// here (createRedisConnection keyPrefix) are disconnected immediately.
// ──────────────────────────────────────────────────────────────────────────────

// Preserve original env so each test can start clean
const ORIGINAL_ENV: Record<string, string | undefined> = {};
for (const key of Object.keys(process.env)) {
  if (key.startsWith('REDIS_') || key === 'USE_REDIS_CLUSTER') {
    ORIGINAL_ENV[key] = process.env[key];
  }
}

function resetEnv(overrides: Record<string, string | undefined> = {}): void {
  for (const key of Object.keys(process.env)) {
    if (key.startsWith('REDIS_') || key === 'USE_REDIS_CLUSTER') {
      delete process.env[key];
    }
  }
  for (const [k, v] of Object.entries(overrides)) {
    if (v !== undefined) process.env[k] = v;
  }
}

afterEach(() => {
  resetEnv(ORIGINAL_ENV);
});

// ──────────────────────────────────────────────────────────────────────────────
// parseRedisNodes
// ──────────────────────────────────────────────────────────────────────────────

describe('parseRedisNodes', () => {
  test('single host uses REDIS_PORT default 6379', () => {
    resetEnv({ REDIS_HOST: 'myhost' });
    expect(parseRedisNodes()).toEqual([{ host: 'myhost', port: 6379 }]);
  });

  test('single host with embedded port', () => {
    resetEnv({ REDIS_HOST: 'myhost:6380' });
    expect(parseRedisNodes()).toEqual([{ host: 'myhost', port: 6380 }]);
  });

  test('bracketed IPv6 host with embedded port', () => {
    resetEnv({ REDIS_HOST: '[2001:db8::1]:6380' });
    expect(parseRedisNodes()).toEqual([
      { host: '2001:db8::1', port: 6380 },
    ]);
  });

  test('unbracketed IPv6 host uses REDIS_PORT default', () => {
    resetEnv({ REDIS_HOST: '2001:db8::1', REDIS_PORT: '6380' });
    expect(parseRedisNodes()).toEqual([
      { host: '2001:db8::1', port: 6380 },
    ]);
  });

  test('comma-separated nodes preserve bracketed IPv6 hosts', () => {
    resetEnv({
      REDIS_HOST: '[2001:db8::1]:6379,node2:6380',
    });
    expect(parseRedisNodes()).toEqual([
      { host: '2001:db8::1', port: 6379 },
      { host: 'node2', port: 6380 },
    ]);
  });

  test('comma-separated hosts without ports', () => {
    resetEnv({ REDIS_HOST: 'node1,node2,node3', REDIS_PORT: '6380' });
    expect(parseRedisNodes()).toEqual([
      { host: 'node1', port: 6380 },
      { host: 'node2', port: 6380 },
      { host: 'node3', port: 6380 },
    ]);
  });

  test('comma-separated hosts with embedded ports', () => {
    resetEnv({ REDIS_HOST: 'node1:6379,node2:6380,node3:6381' });
    expect(parseRedisNodes()).toEqual([
      { host: 'node1', port: 6379 },
      { host: 'node2', port: 6380 },
      { host: 'node3', port: 6381 },
    ]);
  });

  test('trims whitespace around entries', () => {
    resetEnv({ REDIS_HOST: ' node1 , node2 ' });
    expect(parseRedisNodes()).toEqual([
      { host: 'node1', port: 6379 },
      { host: 'node2', port: 6379 },
    ]);
  });

  test('defaults to "redis" when REDIS_HOST is unset', () => {
    resetEnv({});
    expect(parseRedisNodes()).toEqual([{ host: 'redis', port: 6379 }]);
  });
});

// ──────────────────────────────────────────────────────────────────────────────
// isClusterMode
// ──────────────────────────────────────────────────────────────────────────────

describe('isClusterMode', () => {
  test('false by default', () => {
    resetEnv({ REDIS_HOST: 'redis' });
    expect(isClusterMode()).toBe(false);
  });

  test('true when USE_REDIS_CLUSTER=true', () => {
    resetEnv({ REDIS_HOST: 'redis', USE_REDIS_CLUSTER: 'true' });
    expect(isClusterMode()).toBe(true);
  });

  test('true when REDIS_HOST contains a comma', () => {
    resetEnv({ REDIS_HOST: 'node1,node2' });
    expect(isClusterMode()).toBe(true);
  });

  test('false when REDIS_HOST is a single host', () => {
    resetEnv({
      REDIS_HOST: 'single-host:6379',
      USE_REDIS_CLUSTER: 'false',
    });
    expect(isClusterMode()).toBe(false);
  });
});

// ──────────────────────────────────────────────────────────────────────────────
// buildTlsOptions
// ──────────────────────────────────────────────────────────────────────────────

describe('buildTlsOptions', () => {
  test('returns undefined when neither REDIS_TLS nor REDIS_CA is set', () => {
    resetEnv({});
    expect(buildTlsOptions()).toBeUndefined();
  });

  test('returns { rejectUnauthorized: false } when REDIS_TLS=true and no CA', () => {
    resetEnv({ REDIS_TLS: 'true' });
    expect(buildTlsOptions()).toEqual({ rejectUnauthorized: false });
  });

  test('returns { ca } when REDIS_CA points to an existing file', () => {
    const tmpFile = '/tmp/test-redis-ca.pem';
    const pem =
            '-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----\n';
    const { writeFileSync, unlinkSync } =
            require('fs') as typeof import('fs');
    writeFileSync(tmpFile, pem, 'utf8');

    resetEnv({ REDIS_CA: tmpFile });
    try {
      expect(buildTlsOptions()).toEqual({ ca: pem });
    } finally {
      unlinkSync(tmpFile);
    }
  });

  test('CA takes precedence over REDIS_TLS=true', () => {
    const tmpFile = '/tmp/test-redis-ca2.pem';
    const pem =
            '-----BEGIN CERTIFICATE-----\ndata\n-----END CERTIFICATE-----\n';
    const { writeFileSync, unlinkSync } =
            require('fs') as typeof import('fs');
    writeFileSync(tmpFile, pem, 'utf8');

    resetEnv({ REDIS_CA: tmpFile, REDIS_TLS: 'true' });
    try {
      expect(buildTlsOptions()).toEqual({ ca: pem });
    } finally {
      unlinkSync(tmpFile);
    }
  });

  test('returns undefined when REDIS_CA file does not exist', () => {
    resetEnv({ REDIS_CA: '/tmp/does-not-exist-ca-xyz.pem' });
    expect(buildTlsOptions()).toBeUndefined();
  });
});

// ──────────────────────────────────────────────────────────────────────────────
// bullmqPrefix
// ──────────────────────────────────────────────────────────────────────────────

describe('bullmqPrefix', () => {
  test('returns undefined in standalone mode', () => {
    resetEnv({ REDIS_HOST: 'redis' });
    expect(bullmqPrefix()).toBeUndefined();
  });

  test('returns "{codeapi}" in cluster mode via USE_REDIS_CLUSTER', () => {
    resetEnv({ USE_REDIS_CLUSTER: 'true' });
    expect(bullmqPrefix()).toBe('{codeapi}');
  });

  test('returns "{codeapi}" when REDIS_HOST contains multiple nodes', () => {
    resetEnv({ REDIS_HOST: 'node1,node2,node3' });
    expect(bullmqPrefix()).toBe('{codeapi}');
  });

  test('uses REDIS_PREFIX as-is in standalone mode', () => {
    resetEnv({ REDIS_HOST: 'redis', REDIS_PREFIX: 'nos-gpt-staging' });
    expect(bullmqPrefix()).toBe('nos-gpt-staging');
  });

  test('wraps REDIS_PREFIX in a hash tag in cluster mode', () => {
    resetEnv({ USE_REDIS_CLUSTER: 'true', REDIS_PREFIX: 'nos-gpt-staging' });
    expect(bullmqPrefix()).toBe('{nos-gpt-staging}');
  });

  test('does not double-wrap a REDIS_PREFIX that is already a hash tag', () => {
    resetEnv({ USE_REDIS_CLUSTER: 'true', REDIS_PREFIX: '{nos-gpt-staging}' });
    expect(bullmqPrefix()).toBe('{nos-gpt-staging}');
  });

  test('treats a blank REDIS_PREFIX as unset', () => {
    resetEnv({ REDIS_HOST: 'redis', REDIS_PREFIX: '  ' });
    expect(bullmqPrefix()).toBeUndefined();
    resetEnv({ USE_REDIS_CLUSTER: 'true', REDIS_PREFIX: '  ' });
    expect(bullmqPrefix()).toBe('{codeapi}');
  });
});

// ──────────────────────────────────────────────────────────────────────────────
// REDIS_PREFIX key namespace
// ──────────────────────────────────────────────────────────────────────────────

describe('redisPrefix / redisKeyPrefix', () => {
  test('are empty when REDIS_PREFIX is unset or blank', () => {
    resetEnv();
    expect(redisPrefix()).toBeUndefined();
    expect(redisKeyPrefix()).toBe('');
    resetEnv({ REDIS_PREFIX: ' ' });
    expect(redisKeyPrefix()).toBe('');
  });

  test('append a single ":" separator to the namespace', () => {
    resetEnv({ REDIS_PREFIX: ' nos-gpt-staging ' });
    expect(redisPrefix()).toBe('nos-gpt-staging');
    expect(redisKeyPrefix()).toBe('nos-gpt-staging:');
    resetEnv({ REDIS_PREFIX: 'nos-gpt-staging::' });
    expect(redisKeyPrefix()).toBe('nos-gpt-staging:');
  });

  test('accept a hash-tagged value but never emit braces into app keys', () => {
    resetEnv({ REDIS_PREFIX: '{nos-gpt-staging}' });
    expect(redisKeyPrefix()).toBe('nos-gpt-staging:');
  });

  test('reject inner braces that would override per-id hash tags', () => {
    resetEnv({ REDIS_PREFIX: 'a{b}c' });
    expect(() => redisPrefix()).toThrow(/must not contain/);
  });
});

describe('client key prefix helpers', () => {
  const client = (keyPrefix?: string): RedisClient =>
    ({ options: keyPrefix == null ? {} : { keyPrefix } }) as unknown as RedisClient;

  test('are no-ops for an unprefixed client', () => {
    const c = client();
    expect(clientKeyPrefix(c)).toBe('');
    expect(withClientKeyPrefix(c, 'codeapi:x')).toBe('codeapi:x');
    expect(stripClientKeyPrefix(c, 'codeapi:x')).toBe('codeapi:x');
    expect(clientScanPattern(c, 'exec_state:*')).toBe('exec_state:*');
  });

  test('add and strip the client prefix', () => {
    const c = client('dep:');
    expect(withClientKeyPrefix(c, 'codeapi:x')).toBe('dep:codeapi:x');
    expect(stripClientKeyPrefix(c, 'dep:codeapi:x')).toBe('codeapi:x');
    expect(stripClientKeyPrefix(c, 'other:codeapi:x')).toBe('other:codeapi:x');
  });

  test('glob-escape the prefix in SCAN patterns', () => {
    expect(clientScanPattern(client('d*p[1]:'), 'tool_call:*'))
      .toBe('d\\*p\\[1\\]:tool_call:*');
  });
});

describe('createRedisConnection keyPrefix', () => {
  const open = (
    env: Record<string, string>,
    forBullmq?: boolean,
  ): RedisClient => {
    resetEnv({ REDIS_HOST: '127.0.0.1', REDIS_PORT: '1', ...env });
    const c = createRedisConnection(
      { retryStrategy: () => null, enableReadyCheck: false },
      { forBullmq },
    );
    c.on('error', () => {});
    c.disconnect();
    return c;
  };

  test('applies REDIS_PREFIX to standalone and cluster clients', () => {
    expect(open({ REDIS_PREFIX: 'dep' }).options.keyPrefix).toBe('dep:');
    expect(open({ REDIS_PREFIX: 'dep', USE_REDIS_CLUSTER: 'true' }).options.keyPrefix)
      .toBe('dep:');
  });

  test('never sets keyPrefix on BullMQ connections or when unset', () => {
    expect(open({ REDIS_PREFIX: 'dep' }, true).options.keyPrefix).toBeFalsy();
    expect(open({}).options.keyPrefix).toBeFalsy();
  });
});
