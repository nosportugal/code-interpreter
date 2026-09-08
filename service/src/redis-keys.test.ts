import { describe, expect, test, afterEach } from 'bun:test';
import { redisKey, redisKeyPrefix } from './redis-keys';
import { bullmqPrefix } from './redis-connection';

const ORIGINAL_PREFIX = process.env.REDIS_KEY_PREFIX;
const ORIGINAL_CLUSTER = process.env.USE_REDIS_CLUSTER;

afterEach(() => {
    if (ORIGINAL_PREFIX === undefined) delete process.env.REDIS_KEY_PREFIX;
    else process.env.REDIS_KEY_PREFIX = ORIGINAL_PREFIX;
    if (ORIGINAL_CLUSTER === undefined) delete process.env.USE_REDIS_CLUSTER;
    else process.env.USE_REDIS_CLUSTER = ORIGINAL_CLUSTER;
});

describe('redisKeyPrefix', () => {
    test('is empty when unset, leaving keys unchanged', () => {
        delete process.env.REDIS_KEY_PREFIX;
        expect(redisKeyPrefix()).toBe('');
        expect(redisKey('exec_state:abc')).toBe('exec_state:abc');
    });

    test('is empty when set to whitespace', () => {
        process.env.REDIS_KEY_PREFIX = '   ';
        expect(redisKeyPrefix()).toBe('');
    });

    test('appends exactly one separator regardless of how it is written', () => {
        process.env.REDIS_KEY_PREFIX = 'nos-gpt';
        expect(redisKey('session:abc')).toBe('nos-gpt:session:abc');
        process.env.REDIS_KEY_PREFIX = 'nos-gpt:';
        expect(redisKey('session:abc')).toBe('nos-gpt:session:abc');
        process.env.REDIS_KEY_PREFIX = 'nos-gpt::';
        expect(redisKey('session:abc')).toBe('nos-gpt:session:abc');
    });

    test('re-derives when the variable changes', () => {
        process.env.REDIS_KEY_PREFIX = 'a';
        expect(redisKey('k')).toBe('a:k');
        process.env.REDIS_KEY_PREFIX = 'b';
        expect(redisKey('k')).toBe('b:k');
    });

    test('leaves the first cluster hash tag as the slot determinant', () => {
        process.env.REDIS_KEY_PREFIX = 'nos-gpt';
        const prefixed = redisKey('tool_call:session:{exec-1}');
        expect(
            prefixed.slice(prefixed.indexOf('{'), prefixed.indexOf('}') + 1),
        ).toBe('{exec-1}');
    });
});

describe('bullmqPrefix', () => {
    test('keeps the bare cluster hash tag when no key prefix is set', () => {
        delete process.env.REDIS_KEY_PREFIX;
        process.env.USE_REDIS_CLUSTER = 'true';
        expect(bullmqPrefix()).toBe('{codeapi}');
    });

    test('stays undefined in standalone mode when no key prefix is set', () => {
        delete process.env.REDIS_KEY_PREFIX;
        delete process.env.USE_REDIS_CLUSTER;
        expect(bullmqPrefix()).toBeUndefined();
    });

    test('namespaces the queue keyspace without moving the hash slot', () => {
        process.env.REDIS_KEY_PREFIX = 'nos-gpt';
        process.env.USE_REDIS_CLUSTER = 'true';
        expect(bullmqPrefix()).toBe('nos-gpt:{codeapi}');
        delete process.env.USE_REDIS_CLUSTER;
        expect(bullmqPrefix()).toBe('nos-gpt:bull');
    });
});
