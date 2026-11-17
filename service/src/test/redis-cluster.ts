import { spawn } from 'node:child_process';
import { mkdtemp, mkdir, rm } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import path from 'node:path';
import Redis, { Cluster } from 'ioredis';

async function unusedPort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  if (address == null || typeof address === 'string') throw new Error('No test port');
  await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  return address.port;
}

/** Three disposable loopback masters, real slot ownership and Lua enforcement. */
export async function startTestRedisCluster(): Promise<{
  nodes: Array<{ host: string; port: number }>;
  redis: Cluster;
  close(): Promise<void>;
}> {
  const dir = await mkdtemp(path.join(tmpdir(), 'codeapi-cluster-'));
  const processes: Array<{ process: ReturnType<typeof spawn>; exited: Promise<void> }> = [];
  const clients: Redis[] = [];
  const nodes: Array<{ host: string; port: number }> = [];
  const busPorts: number[] = [];
  let redis: Cluster | undefined;
  const close = async (): Promise<void> => {
    redis?.disconnect();
    for (const client of clients) client.disconnect();
    for (const child of processes) child.process.kill('SIGTERM');
    await Promise.all(processes.map(child => child.exited));
    await rm(dir, { recursive: true, force: true });
  };
  try {
    for (let index = 0; index < 3; index++) {
      const port = await unusedPort();
      let busPort = await unusedPort();
      while (busPort === port) busPort = await unusedPort();
      const nodeDir = path.join(dir, String(index));
      await mkdir(nodeDir);
      let failure: Error | undefined;
      const process = spawn('redis-server', [
        '--port', String(port), '--bind', '127.0.0.1',
        '--cluster-enabled', 'yes', '--cluster-port', String(busPort),
        '--cluster-node-timeout', '1000', '--cluster-config-file', 'nodes.conf',
        '--save', '', '--appendonly', 'no', '--dir', nodeDir,
      ], { stdio: 'ignore' });
      process.once('error', error => { failure = error; });
      const exited = new Promise<void>(resolve => {
        process.once('exit', () => resolve());
        process.once('error', () => resolve());
      });
      processes.push({ process, exited });
      const client = new Redis({
        host: '127.0.0.1', port, lazyConnect: true,
        retryStrategy: (): null => null, maxRetriesPerRequest: 0,
      });
      client.on('error', () => {});
      clients.push(client);
      let ready = false;
      for (let attempt = 0; attempt < 100; attempt++) {
        if (failure) throw failure;
        try {
          await client.connect();
          await client.ping();
          ready = true;
          break;
        } catch {
          await new Promise(resolve => setTimeout(resolve, 20));
        }
      }
      if (!ready) throw new Error('Test cluster node did not start; install redis-server');
      nodes.push({ host: '127.0.0.1', port });
      busPorts.push(busPort);
      const start = Math.floor(index * 16384 / 3);
      const end = Math.floor((index + 1) * 16384 / 3) - 1;
      await client.cluster('ADDSLOTS', ...Array.from({ length: end - start + 1 }, (_, slot) => start + slot));
    }
    for (let index = 1; index < nodes.length; index++) {
      await clients[0].call('CLUSTER', 'MEET', nodes[index].host, nodes[index].port, busPorts[index]);
    }
    for (let attempt = 0; attempt < 200; attempt++) {
      const states = await Promise.all(clients.map(client => client.cluster('INFO')));
      if (states.every(state => String(state).includes('cluster_state:ok'))) {
        redis = new Cluster(nodes, { clusterRetryStrategy: (): null => null });
        await redis.ping();
        return { redis, nodes, close };
      }
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    throw new Error('Test Redis Cluster did not converge');
  } catch (error) {
    await close();
    throw error;
  }
}
