import { Redis } from '../../ioredis-sqlite';
import { buildQueue, Queue, ExtendedJob } from './utils';
import _ from 'lodash';

interface RateLimiterOptions {
  max: number;
  duration: number;
  bounceBack?: boolean;
  groupKey?: string;
}

interface QueueOptions {
  limiter: RateLimiterOptions;
}

describe('Rate limiter', () => {
  let queue: Queue;
  let client: Redis;

  beforeEach(() => {
    client = new Redis();
    return client.flushdb().then(() => {
      queue = buildQueue('test rate limiter', {
        limiter: {
          max: 1,
          duration: 1000
        }
      });
      return queue;
    });
  });

  afterEach(() => {
    return queue.close().then(() => {
      return client.quit();
    });
  });

  it('should throw exception if missing duration option', () => {
    expect(() => {
      buildQueue('rate limiter fail', {
        limiter: {
          max: 5
        }
      } as QueueOptions);
    }).toThrow();
  });

  it('should throw exception if missing max option', () => {
    expect(() => {
      buildQueue('rate limiter fail', {
        limiter: {
          duration: 5000
        }
      } as QueueOptions);
    }).toThrow();
  });

  it('should obey the rate limit', (done) => {
    const startTime = new Date().getTime();
    const numJobs = 4;

    queue.process('test', () => {
      return Promise.resolve();
    });

    for (let i = 0; i < numJobs; i++) {
      queue.add({} as any);
    }

    queue.on(
      'completed',
      // after every job has been completed
      _.after(numJobs, () => {
        try {
          const timeDiff = new Date().getTime() - startTime;
          expect(timeDiff).toBeGreaterThan((numJobs - 1) * 1000);
          done();
        } catch (err) {
          done(err);
        }
      })
    );

    queue.on('failed', err => {
      done(err);
    });
  }, 5000);

  // Skip because currently job priority is maintained in a best effort way, but cannot
  // be guaranteed for rate limited jobs.
  it.skip('should obey job priority', async () => {
    const newQueue = buildQueue('test rate limiter', {
      limiter: {
        max: 1,
        duration: 150
      }
    });
    const numJobs = 20;
    const priorityBuckets: Record<number, number> = {
      1: 0,
      2: 0,
      3: 0,
      4: 0
    };

    const numPriorities = Object.keys(priorityBuckets).length;

    newQueue.process('test', (job: ExtendedJob) => {
      const priority = (job as any).opts.priority;

      priorityBuckets[priority] = priorityBuckets[priority] - 1;

      for (let p = 1; p < priority; p++) {
        if (priorityBuckets[p] > 0) {
          const before = JSON.stringify(priorityBucketsBefore);
          const after = JSON.stringify(priorityBuckets);
          throw new Error(
            `Priority was not enforced, job with priority ${priority} was processed before all jobs with priority ${p} were processed. Bucket counts before: ${before} / after: ${after}`
          );
        }
      }
    });

    const result = new Promise<void>((resolve, reject) => {
      newQueue.on('failed', (job, err) => {
        reject(err);
      });

      const afterNumJobs = _.after(numJobs, () => {
        try {
          expect(_.every(priorityBuckets, value => value === 0)).toBe(true);
          resolve();
        } catch (err) {
          reject(err);
        }
      });

      newQueue.on('completed', () => {
        afterNumJobs();
      });
    });

    await newQueue.pause();
    const promises: Promise<any>[] = [];

    for (let i = 0; i < numJobs; i++) {
      const opts = { priority: (i % numPriorities) + 1 };
      priorityBuckets[opts.priority] = priorityBuckets[opts.priority] + 1;
      promises.push(newQueue.add({ id: i } as any, opts));
    }

    const priorityBucketsBefore = _.reduce(
      priorityBuckets,
      (acc: Record<number, number>, value, key) => {
        acc[Number(key)] = value;
        return acc;
      },
      {}
    );

    await Promise.all(promises);

    await newQueue.resume();

    return result;
  }, 60000);

  it('should put a job into the delayed queue when limit is hit', () => {
    const newQueue = buildQueue('test rate limiter', {
      limiter: {
        max: 1,
        duration: 1000
      }
    });

    queue.on('failed', e => {
      fail(e);
    });

    return Promise.all([
      newQueue.add({} as any),
      newQueue.add({} as any),
      newQueue.add({} as any),
      newQueue.add({} as any)
    ]).then(() => {
      Promise.all([
        newQueue.getNextJob(),
        newQueue.getNextJob(),
        newQueue.getNextJob(),
        newQueue.getNextJob()
      ]).then(() => {
        return newQueue.getDelayedCount().then(
          delayedCount => {
            expect(delayedCount).toBe(3);
          },
          () => {
            /*ignore error*/
          }
        );
      });
    });
  });

  it('should not put a job into the delayed queue when discard is true', () => {
    const newQueue = buildQueue('test rate limiter', {
      limiter: {
        max: 1,
        duration: 1000,
        bounceBack: true
      }
    });

    newQueue.on('failed', e => {
      fail(e);
    });
    return Promise.all([
      newQueue.add({} as any),
      newQueue.add({} as any),
      newQueue.add({} as any),
      newQueue.add({} as any)
    ]).then(() => {
      Promise.all([
        newQueue.getNextJob(),
        newQueue.getNextJob(),
        newQueue.getNextJob(),
        newQueue.getNextJob()
      ]).then(() => {
        return newQueue.getDelayedCount().then(delayedCount => {
          expect(delayedCount).toBe(0);
          return newQueue.getActiveCount().then(waitingCount => {
            expect(waitingCount).toBe(1);
          });
        });
      });
    });
  });

  it('should rate limit by grouping', async () => {
    jest.setTimeout(20000);
    const numGroups = 4;
    const numJobs = 20;
    const startTime = Date.now();

    const rateLimitedQueue = buildQueue('test rate limiter with group', {
      limiter: {
        max: 1,
        duration: 1000,
        groupKey: 'accountId'
      }
    });

    rateLimitedQueue.process('test', () => {
      return Promise.resolve();
    });

    const completed: Record<string, number[]> = {};

    const running = new Promise<void>((resolve, reject) => {
      const afterJobs = _.after(numJobs, () => {
        try {
          const timeDiff = Date.now() - startTime;
          expect(timeDiff).toBeGreaterThanOrEqual(numGroups * 1000);
          expect(timeDiff).toBeLessThan((numGroups + 1) * 1500);

          for (const group in completed) {
            let prevTime = completed[group][0];
            for (let i = 1; i < completed[group].length; i++) {
              const diff = completed[group][i] - prevTime;
              expect(diff).toBeLessThan(2100);
              expect(diff).toBeGreaterThanOrEqual(900);
              prevTime = completed[group][i];
            }
          }
          resolve();
        } catch (err) {
          reject(err);
        }
      });

      rateLimitedQueue.on('completed', ({ id }: { id: string }) => {
        const group = _.last(id.split(':')) || '';
        completed[group] = completed[group] || [];
        completed[group].push(Date.now());

        afterJobs();
      });

      rateLimitedQueue.on('failed', async err => {
        await queue.close();
        reject(err);
      });
    });

    for (let i = 0; i < numJobs; i++) {
      rateLimitedQueue.add({ accountId: i % numGroups } as any);
    }

    await running;
    await rateLimitedQueue.close();
  });
});
