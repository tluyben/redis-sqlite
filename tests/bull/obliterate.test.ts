import { Redis } from '../../ioredis-sqlite';
import { buildQueue, Queue, ExtendedJob } from './utils';
import { v4 as uuid } from 'uuid';
import delay from 'delay';

describe('Obliterate', () => {
  let queue: Queue;

  beforeEach(() => {
    queue = buildQueue('cleaner' + uuid());
  });

  afterEach(async () => {
    jest.setTimeout(queue.settings.stalledInterval * (1 + queue.settings.maxStalledCount));
    await queue.close();
  });

  it('should obliterate an empty queue', async () => {
    await queue.obliterate();

    const client = await queue.client;
    const keys = await client.keys(`bull:${queue.name}*`);

    expect(keys.length).toBe(0);
  });

  it('should obliterate a queue with jobs in different statuses', async () => {
    await queue.add('test', { foo: 'bar' });
    await queue.add('test', { foo: 'bar2' });
    await queue.add('test', { foo: 'bar3' }, { delay: 5000 });
    const job = await queue.add('test', { qux: 'baz' });

    let first = true;
    queue.process('test', async () => {
      if (first) {
        first = false;
        throw new Error('failed first');
      }
      return delay(250);
    });

    await job.finished();

    await queue.obliterate();
    const client = await queue.client;
    const keys = await client.keys(`bull:${queue.name}*`);
    expect(keys.length).toBe(0);
  });

  it('should raise exception if queue has active jobs', async () => {
    await queue.add('test', { foo: 'bar' });
    const job = await queue.add('test', { qux: 'baz' });

    await queue.add('test', { foo: 'bar2' });
    await queue.add('test', { foo: 'bar3' }, { delay: 5000 });

    let first = true;
    queue.process('test', async () => {
      if (first) {
        first = false;
        throw new Error('failed first');
      }
      return delay(250);
    });

    await job.finished();

    try {
      await queue.obliterate();
    } catch (err) {
      const client = await queue.client;
      const keys = await client.keys(`bull:${queue.name}*`);
      expect(keys.length).not.toBe(0);
      return;
    }

    throw new Error('Should raise an exception if there are active jobs');
  });

  it('should obliterate if queue has active jobs using "force"', async () => {
    await queue.add('test', { foo: 'bar' });
    const job = await queue.add('test', { qux: 'baz' });

    await queue.add('test', { foo: 'bar2' });
    await queue.add('test', { foo: 'bar3' }, { delay: 5000 });

    let first = true;
    queue.process('test', async () => {
      if (first) {
        first = false;
        throw new Error('failed first');
      }
      return delay(250);
    });
    await job.finished();

    await queue.obliterate({ force: true });
    const client = await queue.client;
    const keys = await client.keys(`bull:${queue.name}*`);
    expect(keys.length).toBe(0);
  });

  it('should remove repeatable jobs', async () => {
    await queue.add(
      'test',
      { foo: 'bar' },
      {
        repeat: {
          every: 1000
        }
      }
    );

    const repeatableJobs = await queue.getRepeatableJobs();
    expect(repeatableJobs).toHaveLength(1);

    await queue.obliterate();
    const client = await queue.client;
    const keys = await client.keys(`bull:${queue.name}:*`);
    expect(keys.length).toBe(0);
  });

  it('should remove job logs', async () => {
    const job = await queue.add('test', {});

    queue.process('test', async job => {
      return job.log('Lorem Ipsum Dolor Sit Amet');
    });

    await job.finished();

    await queue.obliterate({ force: true });

    const { logs } = await queue.getJobLogs(job.id);
    expect(logs).toHaveLength(0);
  });

  it('should obliterate a queue with high number of jobs in different statuses', async () => {
    const arr1: Promise<ExtendedJob>[] = [];
    for (let i = 0; i < 300; i++) {
      arr1.push(queue.add('test', { foo: `barLoop${i}` }));
    }

    const [lastCompletedJob] = (await Promise.all(arr1)).splice(-1);

    let fail = false;
    queue.process('test', async () => {
      if (fail) {
        throw new Error('failed job');
      }
    });

    await lastCompletedJob.finished();

    fail = true;

    const arr2: Promise<ExtendedJob>[] = [];
    for (let i = 0; i < 300; i++) {
      arr2.push(queue.add('test', { foo: `barLoop${i}` }));
    }

    const [lastFailedJob] = (await Promise.all(arr2)).splice(-1);

    await expect(lastFailedJob.finished()).rejects.toThrow();

    const arr3: Promise<ExtendedJob>[] = [];
    for (let i = 0; i < 1623; i++) {
      arr3.push(queue.add('test', { foo: `barLoop${i}` }, { delay: 10000 }));
    }
    await Promise.all(arr3);

    await queue.obliterate();
    const client = await queue.client;
    const keys = await client.keys(`bull:${queue.name}*`);
    expect(keys.length).toBe(0);
  }, 20000);
});
