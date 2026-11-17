import { afterAll, afterEach, beforeAll, describe, expect, mock, test } from 'bun:test';
import { createHash, randomBytes } from 'node:crypto';
import { Cluster } from 'ioredis';
import {
  createBridgeIdentity,
  signBridgeRequest,
  signBridgeRecovery,
  signBridgeRecoveryStart,
} from '../../../packages/code/src/identity';
import { signEnrollmentRedemption } from '../../../packages/code/src/browser-enrollment-protocol';
import type { BrowserEnrollmentApproval } from '../../../packages/code/src/browser-enrollment-protocol';
import { scanKeyBatches, withClientKeyPrefix } from '../redis-connection';
import { startTestRedisCluster } from '../test/redis-cluster';
import { RedisWorkspaceRequests } from '../workspace-tools/requests';
import { RedisBridgePairingStore } from './pairing';
import type { BridgeWorkerCredential } from './pairing';
import { RedisBridgeStore } from './store';
import type { CodeBridgeAssignment } from './store';
import { assertBridgeRedisSchema, BRIDGE_REDIS_PREFIX as PREFIX, WORKSPACE_REQUEST_REDIS_PREFIX } from './redis-keys';

const integration = process.env.BRIDGE_REDIS_CLUSTER_TEST === '1' ? describe : describe.skip;

integration('bridge on a real three-master Redis Cluster', () => {
  let cluster: Awaited<ReturnType<typeof startTestRedisCluster>>;
  const clients: Cluster[] = [];
  const identity = createBridgeIdentity();
  const serverId = 'https://code.example.test';
  const workerId = 'cluster-worker';
  const incarnationId = 'cluster-incarnation-01';
  const owner = { tenantId: 'tenant', userId: 'owner' };
  const binding = { tenantId: owner.tenantId, principal: { type: 'user' as const, id: owner.userId } };

  beforeAll(async () => {
    expect(process.env.USE_REDIS_CLUSTER).toBe('true');
    cluster = await startTestRedisCluster();
  }, 20_000);
  afterEach(async () => {
    for (const client of clients.splice(0)) client.disconnect();
    // This helper created and owns all three Redis processes.
    await Promise.all(cluster.redis.nodes('master').map(node => node.flushall()));
  });
  afterAll(async () => { await cluster?.close(); });

  function client(prefix: string): Cluster {
    const redis = new Cluster(cluster.nodes, { keyPrefix: prefix, clusterRetryStrategy: (): null => null });
    clients.push(redis);
    return redis;
  }
  function pairings(redis: Cluster): RedisBridgePairingStore {
    return new RedisBridgePairingStore(redis, 600, 900, 5_000, '', { serverId });
  }
  function authorize(store: RedisBridgePairingStore, credential: BridgeWorkerCredential): ReturnType<RedisBridgePairingStore['authorize']> {
    const proof = {
      credential: credential.credential, method: 'POST', path: '/v1/bridge/workers/register',
      timestamp: new Date().toISOString(), nonce: randomBytes(24).toString('base64url'), body: '{}',
    };
    return store.authorize({ ...proof, workerId, signature: signBridgeRequest(identity.privateKey, proof) });
  }

  test('negative controls reject both original cross-slot scripts', async () => {
    for (const keys of [
      ['codeapi-staging:codeapi:bridge:v1:migration:legacy-pairing-scanned:code-worker',
        'codeapi-staging:codeapi:bridge:v1:migration:legacy-pairing-scanned:code-worker:claim'],
      ['codeapi:bridge:v1:pairing-index:worker', 'codeapi:bridge:v1:pairing:digest'],
    ]) {
      await expect(cluster.redis.eval('return 1', 2, ...keys)).rejects.toThrow('CROSSSLOT');
    }
  });

  test.each(['', 'bridge*[scope]:'])('pair, rotate, recover, revoke and fence atomically (prefix=%s)', async prefix => {
    const redis = client(prefix);
    const store = pairings(redis);
    const worker = new RedisBridgeStore(redis);
    await assertBridgeRedisSchema(redis);
    const superseded = await store.issue(workerId, binding);
    const pairing = await store.issue(workerId, binding);
    await expect(store.redeem({ workerId, code: superseded.code, publicKey: identity.publicKey }))
      .rejects.toMatchObject({ code: 'PAIRING_INVALID' });
    const redemptions = await Promise.allSettled([0, 1].map(() =>
      store.redeem({ workerId, code: pairing.code, publicKey: identity.publicKey })));
    const successful = redemptions.filter(result => result.status === 'fulfilled');
    expect(successful).toHaveLength(1);
    const credential = successful[0];
    const authenticated = await authorize(store, credential.value);
    await worker.register({
      protocolVersion: 1, incarnationId, ...authenticated,
      capabilities: { statefulWorkspace: false, sandboxProfile: 'native-srt', runtimes: ['bash'] },
    }, authenticated.credentialId);
    const rotated = await store.rotate(workerId, authenticated.credentialId);
    expect((await authorize(store, rotated)).identityId).toBe(authenticated.identityId);
    const proof = {
      operation: 'credential.challenge' as const, serverId, workerId,
      timestamp: new Date().toISOString(), nonce: randomBytes(32).toString('base64url'),
    };
    const challenge = await store.createRecoveryChallenge(workerId, proof, signBridgeRecoveryStart(identity.privateKey, proof));
    const recovered = await store.recoverCredential(workerId, challenge, signBridgeRecovery(identity.privateKey, challenge));
    expect((await authorize(store, recovered)).identityId).toBe(authenticated.identityId);
    await expect(authorize(pairings(client(`${prefix}other:`)), recovered))
      .rejects.toMatchObject({ code: 'CREDENTIAL_INVALID' });
    await expect(store.recoverCredential(workerId, challenge, signBridgeRecovery(identity.privateKey, challenge)))
      .rejects.toMatchObject({ code: 'CHALLENGE_INVALID' });
    const replacement = await authorize(store, recovered);
    await worker.register({
      protocolVersion: 1, incarnationId: 'replacement-incarnation', ...replacement,
      capabilities: { statefulWorkspace: false, sandboxProfile: 'native-srt', runtimes: ['bash'] },
    }, replacement.credentialId);
    expect(await redis.get(`${PREFIX}:worker:${workerId}:incarnation:${incarnationId}:fenced`)).toBe('1');
    await store.revoke(workerId);
    await expect(authorize(store, recovered)).rejects.toMatchObject({ code: 'CREDENTIAL_INVALID' });
    const digest = createHash('sha256').update(recovered.credential).digest('hex');
    expect(await redis.get(`${PREFIX}:credential:${digest}`)).toBeNull();
    expect(await redis.get(`${PREFIX}:worker:${workerId}:incarnation:replacement-incarnation:fenced`)).toBe('1');
    await expect(worker.register({
      protocolVersion: 1, incarnationId, ...authenticated,
      capabilities: { statefulWorkspace: false, sandboxProfile: 'native-srt', runtimes: ['bash'] },
    }, authenticated.credentialId)).rejects.toMatchObject({ code: 'WORKER_UNAUTHORIZED' });
  });

  test('browser enrollment keeps prefixed stored-key comparisons and replay protection', async () => {
    const store = pairings(client('browser:'));
    const approval: BrowserEnrollmentApproval = {
      protocolVersion: 1, operation: 'enrollment.redeem', serverId,
      instance: 'https://chat.example.test', attemptId: randomBytes(32).toString('base64url'),
      requestHash: 'a'.repeat(64), workerId, publicKey: identity.publicKey, binding,
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
    };
    await store.issueBrowserEnrollment(approval);
    const { operation, instance, attemptId, requestHash, expiresAt } = approval;
    const contract = { operation, serverId, instance, attemptId, requestHash, workerId, expiresAt };
    expect((await store.browserEnrollmentStatus({ protocolVersion: 1, ...contract })).status).toBe('pending');
    const proof = { protocolVersion: 1 as const, ...contract, signature: signEnrollmentRedemption(identity.privateKey, contract) };
    const credential = await store.redeemBrowserEnrollment(proof, 'administrator-test-secret');
    expect(await store.redeemBrowserEnrollment(proof, 'administrator-test-secret')).toEqual(credential);
    expect((await store.browserEnrollmentStatus({ protocolVersion: 1, ...contract })).status).toBe('completed');
    await store.revoke(workerId);
    await expect(store.redeemBrowserEnrollment(proof, 'administrator-test-secret'))
      .rejects.toMatchObject({ code: 'ENROLLMENT_INVALID' });
  });

  test.each([1, 2])('durable requests, admission, slots, settlement and cleanup share a slot (capacity=%s)', async capacity => {
    const redis = client('requests:');
    const bridge = new RedisBridgeStore(redis, 600, 1000, 2);
    const generation = await bridge.register({
      protocolVersion: 1, workerId, incarnationId, binding,
      capabilities: {
        sandboxProfile: 'native-srt', statefulWorkspace: false, runtimes: ['bash'],
        ...(capacity > 1 ? { workspaceLeaseSlots: capacity, requiresReadyConfirmation: true } : {}),
        workspaceTools: { protocolVersion: 1, operations: ['read_file'], workspaces: [{ id: 'primary' }] },
      },
    });
    if (capacity > 1) await bridge.confirmReady(workerId, incarnationId, generation);
    const requests = new RedisWorkspaceRequests(redis, bridge);
    const requestId = 'request-cluster-000001';
    const submit = (coordinator = requests): ReturnType<RedisWorkspaceRequests['submit']> => coordinator.submit({
      owner, requestId, workerId, requireTenantBinding: true,
      request: { protocolVersion: 1, operation: 'read_file', workspaceId: 'primary', path: 'README.md' },
      queueWaitMs: 30_000, executionTimeoutMs: 30_000,
    });
    await submit();
    await submit(new RedisWorkspaceRequests(redis, bridge));
    expect(await requests.get({ ...owner, tenantId: 'different' }, requestId)).toBeUndefined();
    await requests.reconcile();
    const assignment = await bridge.lease(workerId, incarnationId, 0, undefined, undefined, capacity > 1 ? 0 : undefined);
    expect(assignment).toBeDefined();
    if (assignment == null) throw new Error('No cluster assignment');
    await bridge.acknowledgeLease(workerId, incarnationId, assignment.assignmentId, assignment.generation, assignment.leaseToken);
    await bridge.settle(workerId, assignment.assignmentId, {
      protocolVersion: 1, incarnationId, generation: assignment.generation, leaseToken: assignment.leaseToken,
      status: 'fulfilled',
      result: { protocolVersion: 1, operation: 'read_file', workspaceId: 'primary', path: 'README.md',
        content: 'cluster result', startLine: 1, endLine: 1, truncated: false },
    });
    // Reconciliation is scheduled in Redis, including when a replica restarts.
    const active = `${WORKSPACE_REQUEST_REDIS_PREFIX}:active`;
    for (const key of await redis.zrange(active, 0, -1)) await redis.zadd(active, 0, key);
    await new RedisWorkspaceRequests(redis, bridge).reconcile();
    expect(await requests.get(owner, requestId)).toMatchObject({ state: 'completed', result: { content: 'cluster result' } });
    if (capacity > 1) {
      await bridge.confirmWorkspaceCleanup(workerId, assignment.assignmentId, {
        protocolVersion: 1, incarnationId, generation: assignment.generation, leaseToken: assignment.leaseToken,
        status: 'rejected', error: 'local cleanup confirmed',
      });
    }
    expect(await redis.zcard(active)).toBe(0);
    expect(await redis.get(`${PREFIX}:worker:${workerId}:lock`)).toBeNull();
    await submit(new RedisWorkspaceRequests(redis, bridge));
    expect(await redis.zcard(active)).toBe(0);
  });

  test('linked-worktree indexes retain prefixed quarantine fences when admitting a checkout', async () => {
    const redis = client('lanes:');
    const bridge = new RedisBridgeStore(redis, 60, 1000, 2);
    const generation = await bridge.register({
      protocolVersion: 1, workerId, incarnationId,
      capabilities: {
        statefulWorkspace: false, sandboxProfile: 'native-srt', runtimes: [],
        workspaceLeaseSlots: 2, requiresReadyConfirmation: true,
        workspaceTools: {
          protocolVersion: 1, operations: ['read_file'],
          workspaces: [{ id: 'repo', workspaceScopes: ['git_linked_worktree'] }],
        },
      },
    });
    await bridge.confirmReady(workerId, incarnationId, generation);
    const controller = new AbortController();
    const pending: Promise<unknown>[] = [];
    const dispatch = (worktree?: string): ReturnType<RedisBridgeStore['dispatchWorkspaceTool']> => {
      const completion = bridge.dispatchWorkspaceTool({
        workerId, deadlineAtMs: Date.now() + 3000, signal: controller.signal,
        request: { protocolVersion: 1, operation: 'read_file', workspaceId: 'repo', path: 'probe',
          ...(worktree === undefined ? {} : { worktree }) },
      });
      void completion.catch(() => undefined);
      pending.push(completion);
      return completion;
    };
    const finish = async (assignment: CodeBridgeAssignment): Promise<void> => {
      const settlement = {
        protocolVersion: 1 as const, incarnationId, generation: assignment.generation,
        leaseToken: assignment.leaseToken, status: 'rejected' as const, error: 'test finished',
      };
      await bridge.acknowledgeLease(workerId, incarnationId, assignment.assignmentId, assignment.generation, assignment.leaseToken);
      await bridge.settle(workerId, assignment.assignmentId, settlement);
      await bridge.confirmWorkspaceCleanup(workerId, assignment.assignmentId, settlement);
    };
    try {
      const lane = dispatch('task-a');
      const assignment = await bridge.lease(workerId, incarnationId, 1000, undefined, undefined, 0);
      if (assignment == null) throw new Error('No lane assignment');
      await finish(assignment);
      await lane;
      const laneHash = createHash('sha256').update('native-workspace:\0linked-worktree\0repo\0task-a').digest('hex');
      const fence = `${PREFIX}:worker:${workerId}:workspace:${laneHash}:quarantined`;
      await redis.set(fence, 'quarantined:cleanup-unacknowledged');
      await expect(dispatch()).rejects.toMatchObject({ code: 'WORKSPACE_QUARANTINED' });
      expect(await redis.get(fence)).toBe('quarantined:cleanup-unacknowledged');
    } finally {
      controller.abort();
      await Promise.allSettled(pending);
    }
  });

  test('schema guard scans every master and never touches another deployment', async () => {
    const redis = client('legacy*[scope]:');
    const other = client('other:');
    const legacy = 'codeapi:bridge:v1:enrollment:old-worker';
    await other.set(legacy, 'other deployment enrollment');
    await assertBridgeRedisSchema(redis);
    await redis.set(legacy, 'retain this authorization state');
    await expect(assertBridgeRedisSchema(redis)).rejects.toThrow('offline migration');
    expect(await redis.get(legacy)).toBe('retain this authorization state');
    expect(await other.get(legacy)).toBe('other deployment enrollment');
    const keys: string[] = [];
    for await (const batch of scanKeyBatches(redis, 'codeapi:bridge:v1:*', 1)) keys.push(...batch);
    expect(keys).toEqual([legacy]);
    await redis.del(legacy);
    // Place the only legacy key on each master in turn.
    for (let shard = 0; shard < 3; shard++) {
      let key = '';
      for (let candidate = 0; candidate < 1000; candidate++) {
        const proposed = `codeapi:workspace-requests:v1:old-${candidate}`;
        const slot = Number(await cluster.redis.cluster('KEYSLOT', withClientKeyPrefix(redis, proposed)));
        if (slot >= Math.floor(shard * 16384 / 3) && slot < Math.floor((shard + 1) * 16384 / 3)) {
          key = proposed;
          break;
        }
      }
      expect(key).not.toBe('');
      await redis.set(key, 'legacy request');
      await expect(assertBridgeRedisSchema(redis)).rejects.toThrow('offline migration');
      expect(await redis.get(key)).toBe('legacy request');
      await redis.del(key);
    }
  });

  test('replay cleanup reads multiple execution slots without cross-slot MGET', async () => {
    const redis = client('replay:');
    // Replace only queue construction, not Redis: the module uses this real
    // Cluster connection for every read, script and transaction.
    mock.module('../queue', (): { connection: Cluster } => ({ connection: redis }));
    const replay = await import('../service/replay-state');
    for (const id of ['stale-a', 'stale-b', 'fresh']) {
      await replay.setExecutionState({
        execution_id: id, session_id: `session-${id}`, userId: 'owner',
        startTime: Date.now() - (id === 'fresh' ? 0 : (replay.EXECUTION_STATE_TTL + 60) * 1000),
        mode: 'replay',
      });
    }
    await expect(redis.mget('exec_state:{stale-a}', 'exec_state:{stale-b}')).rejects.toThrow('CROSSSLOT');
    expect(await replay.cleanupStaleExecutions()).toBe(2);
    expect(await replay.getExecutionState('stale-a')).toBeNull();
    expect(await replay.getExecutionState('stale-b')).toBeNull();
    expect(await replay.getExecutionState('fresh')).not.toBeNull();
  });
});
