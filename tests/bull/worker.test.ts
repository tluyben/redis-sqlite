import { Redis } from '../../ioredis-sqlite';
import { buildQueue, Queue, ExtendedJob } from './utils';
import Bull from 'bull';
import sinon from 'sinon';

type ProcessFunction = (job: ExtendedJob, done?: (error?: Error | null) => void) => void | Promise<any>;

describe('Worker', () => {
  let queue: Queue;
  let client: Redis;

  beforeEach(() => {
    client = new Redis();
    return client.flushdb().then(() => {
      queue = buildQueue('test worker');
      return queue;
    });
  });

  afterEach(async () => {
    await queue.close();
    await client.flushdb();
    return client.quit();
  });

  it('should process a job', async () => {
    const job = await queue.add({ foo: 'bar' } as any);

    await new Promise<void>((resolve) => {
      const processor: ProcessFunction = (job, done = () => {}) => {
        expect(job.data.foo).toBe('bar');
        done();
        resolve();
      };
      queue.process('test', processor);
    });
  });

  it('should process a job that updates progress', async () => {
    const job = await queue.add({ foo: 'bar' } as any);

    await new Promise<void>((resolve, reject) => {
      const processor: ProcessFunction = async (job, done = () => {}) => {
        expect(job.data.foo).toBe('bar');
        try {
          await job.progress(42);
          done();
          resolve();
        } catch (err) {
          reject(err);
        }
      };
      queue.process('test', processor);
    });
  });

  it('should process a job that returns data in the process handler', async () => {
    await queue.add({ foo: 'bar' } as any);

    await new Promise<void>((resolve, reject) => {
      const processor: ProcessFunction = async (job, done = () => {}) => {
        expect(job.data.foo).toBe('bar');
        try {
          await job.moveToCompleted('test data', true);
          const gotJob = await queue.getJob(job.id);
          expect(gotJob?.returnvalue).toBe('test data');
          done();
          resolve();
        } catch (err) {
          reject(err);
        }
      };
      queue.process('test', processor);
    });
  });

  it('should process stalled jobs when starting a queue', async () => {
    const queueStalled = buildQueue('test stalled');
    const stalledQueue = buildQueue('test stalled');

    const job = await queueStalled.add({ foo: 'bar' } as any);

    await new Promise<void>((resolve) => {
      const processStarted = new Promise<void>((resolve) => {
        const processor: ProcessFunction = async (job, done = () => {}) => {
          resolve();
          await delay(250);
          done();
        };
        queueStalled.process('test', processor);
      });

      processStarted.then(async () => {
        await delay(500);

        // Create a worker using the underlying Bull queue
        const bullQueue = stalledQueue as unknown as Bull.Queue;
        const worker = new (Bull as any).Worker(bullQueue.name, async (job: any) => {
          return Promise.resolve();
        });

        worker.on('completed', async () => {
          const jobs = await stalledQueue.getCompleted();
          expect(jobs).toHaveLength(1);
          const job = await stalledQueue.getJob(jobs[0].id);
          expect(job?.finishedOn).toBeDefined();
          resolve();
        });
      });
    });

    await queueStalled.close();
    await stalledQueue.close();
  });

  it('should process jobs in priority order', async () => {
    const jobs = await Promise.all([
      queue.add({ p: 1 } as any, { priority: 1 }),
      queue.add({ p: 2 } as any, { priority: 2 }),
      queue.add({ p: 3 } as any, { priority: 3 })
    ]);

    let currentPriority = 3;

    await new Promise<void>((resolve, reject) => {
      const processor: ProcessFunction = async (job, done = () => {}) => {
        try {
          expect(job.id).toBeDefined();
          expect(job.data.p).toBe(currentPriority--);
          if (currentPriority === 0) {
            done();
            resolve();
          } else {
            done();
          }
        } catch (err) {
          reject(err);
        }
      };
      queue.process('test', processor);
    });
  });

  it('should process several stalled jobs when starting several queues', async () => {
    const NUM_QUEUES = 10;
    const NUM_JOBS = 20;

    const queues = Array.from({ length: NUM_QUEUES }, () => buildQueue('test stalled'));
    const jobs = Array.from({ length: NUM_JOBS }, (_, i) => ({ foo: i } as any));

    await Promise.all(jobs.map(job => queues[0].add(job)));

    await new Promise<void>((resolve) => {
      let processedJobs = 0;
      for (const queue of queues) {
        const processor: ProcessFunction = async (job, done = () => {}) => {
          processedJobs++;
          if (processedJobs === NUM_JOBS) {
            done();
            resolve();
          } else {
            done();
          }
        };
        queue.process('test', processor);
      }
    });

    await Promise.all(queues.map(queue => queue.close()));
  });
});

function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
