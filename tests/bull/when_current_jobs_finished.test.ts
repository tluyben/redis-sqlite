import { Redis } from '../../ioredis-sqlite';
import { buildQueue, Queue, cleanupQueues, newQueue } from './utils';
import delay from 'delay';
import sinon from 'sinon';

describe('.whenCurrentJobsFinished', () => {
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

  it('should handle queue with no processor', async () => {
    const queue = await newQueue('test');
    expect(await queue.whenCurrentJobsFinished()).toBeUndefined();
  });

  it('should handle queue with no jobs', async () => {
    const queue = await newQueue('test');
    queue.process('test', (job, done = () => {}) => {
      done();
      return Promise.resolve();
    });
    expect(await queue.whenCurrentJobsFinished()).toBeUndefined();
  });

  it('should wait for job to complete', async () => {
    const queue = await newQueue('test');
    await queue.add({} as any);

    let finishJob: () => void;

    // wait for job to be active
    await new Promise<void>(resolve => {
      queue.process('test', (job, done = () => {}) => {
        resolve();
        return new Promise<void>(resolve => {
          finishJob = () => {
            done();
            resolve();
          };
        });
      });
    });

    let isFulfilled = false;
    const finished = queue.whenCurrentJobsFinished().then(() => {
      isFulfilled = true;
    });

    await delay(100);
    expect(isFulfilled).toBe(false);

    finishJob!();
    expect(await finished).toBeUndefined();
  });

  it('should wait for all jobs to complete', async () => {
    const queue = await newQueue('test');

    // add multiple jobs to queue
    await queue.add({} as any);
    await queue.add({} as any);

    let finishJob1: () => void;
    let finishJob2: () => void;

    // wait for all jobs to be active
    await new Promise<void>(resolve => {
      let callCount = 0;
      queue.process('test', (job, done = () => {}) => {
        callCount++;
        if (callCount === 1) {
          return new Promise<void>(resolve => {
            finishJob1 = () => {
              done();
              resolve();
            };
          });
        }

        resolve();
        return new Promise<void>(resolve => {
          finishJob2 = () => {
            done();
            resolve();
          };
        });
      });
    });

    let isFulfilled = false;
    const finished = queue.whenCurrentJobsFinished().then(() => {
      isFulfilled = true;
    });

    finishJob2!();
    await delay(100);

    expect(isFulfilled).toBe(false);

    finishJob1!();
    await delay(100);
    expect(await finished).toBeUndefined();
  });

  it('should wait for job to fail', async () => {
    const queue = await newQueue('test');
    await queue.add({} as any);

    let rejectJob: (error: Error) => void;

    // wait for job to be active
    await new Promise<void>(resolve => {
      queue.process('test', (job, done = () => {}) => {
        resolve();
        return new Promise<void>((resolve, reject) => {
          rejectJob = (error: Error) => {
            done(error);
            reject(error);
          };
        });
      });
    });

    let isFulfilled = false;
    const finished = queue.whenCurrentJobsFinished().then(() => {
      isFulfilled = true;
    });

    await delay(100);
    expect(isFulfilled).toBe(false);

    rejectJob!(new Error('test error'));
    expect(await finished).toBeUndefined();
  });
});
