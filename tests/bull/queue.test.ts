import { Redis } from '../../ioredis-sqlite';
import { buildQueue, Queue, cleanupQueues } from './utils';
import delay from 'delay';
import sinon from 'sinon';

describe('Queue', () => {
  let client: Redis;

  beforeEach(() => {
    client = new Redis();
    return client.flushdb();
  });

  afterEach(async () => {
    sinon.restore();
    await cleanupQueues();
    await client.flushdb();
    return client.quit();
  });

  it('should create a queue with default settings', () => {
    const queue = buildQueue();
    expect(queue.name).toBe('test queue');
  });

  it('should create a queue with custom settings', () => {
    const queue = buildQueue('custom queue');
    expect(queue.name).toBe('custom queue');
  });
});
