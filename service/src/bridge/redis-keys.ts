import { isClusterMode, scanKeyBatches, type RedisClient } from '../redis-connection';

// Pairing digests, worker IDs and durable request IDs participate in the same
// atomic transitions. Per-worker tags cannot cover those transitions.
export const BRIDGE_REDIS_PREFIX = isClusterMode()
  ? '{codeapi:bridge:v1}'
  : 'codeapi:bridge:v1';
export const WORKSPACE_REQUEST_REDIS_PREFIX = isClusterMode()
  ? `${BRIDGE_REDIS_PREFIX}:workspace-requests`
  : 'codeapi:workspace-requests:v1';

/** Never start a second, empty authorization/fencing namespace unnoticed. */
export async function assertBridgeRedisSchema(redis: RedisClient): Promise<void> {
  if (!isClusterMode()) return;
  for (const prefix of ['codeapi:bridge:v1', 'codeapi:workspace-requests:v1']) {
    for await (const keys of scanKeyBatches(redis, `${prefix}:*`)) {
      if (keys.length > 0) {
        throw new Error(
          'Legacy untagged bridge Redis state requires an offline migration or an explicit fresh-deployment/re-pairing cutover. ' +
          'Stop all old replicas and follow docs/remote-bridge/README.md#redis-cluster-upgrades; do not flush shared Redis.',
        );
      }
    }
  }
}
