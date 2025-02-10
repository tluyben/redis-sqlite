import { Redis } from '../../ioredis-sqlite';
import { buildQueue, Queue, ExtendedJob } from './utils';
import Bull from 'bull';
import delay from 'delay';
import { v4 as uuid } from 'uuid';

describe('Job', () => {
  let queue: Queue;
  let client: Redis;

  beforeEach(async () => {
    client = new Redis();
    await client.flushdb();
  });

  beforeEach(() => {
    queue = new Bull('test-' + uuid(), {
      redis: { port: 6379, host: '127.0.0.1' }
    }) as unknown as Queue;
  });

  afterEach(async () => {
    jest.setTimeout(queue.settings.stalledInterval * (1 + queue.settings.maxStalledCount));
    await queue.close();
    await client.quit();
  });

  describe('.create', () => {
    let job: ExtendedJob;
    let data: { foo: string };
    let opts: { testOpt: string; jobId?: string };

    beforeEach(async () => {
      data = { foo: 'bar' };
      opts = { testOpt: 'enabled' };

      job = await (Bull as any).Job.create(queue as any, data, opts);
    });

    it('returns a promise for the job', () => {
      expect(job).toHaveProperty('id');
      expect(job).toHaveProperty('data');
    });

    it('should not modify input options', () => {
      expect(opts).not.toHaveProperty('jobId');
    });

    it('saves the job in redis', async () => {
      const storedJob = await (Bull as any).Job.fromId(queue as any, job.id);
      expect(storedJob).toHaveProperty('id');
      expect(storedJob).toHaveProperty('data');

      expect(storedJob.data.foo).toBe('bar');
      expect(storedJob.opts).toBeInstanceOf(Object);
      expect(storedJob.opts.testOpt).toBe('enabled');
    });

    it('should use the custom jobId if one is provided', async () => {
      const customJobId = 'customjob';
      const createdJob = await (Bull as any).Job.create(queue as any, data, { jobId: customJobId });
      expect(createdJob.id).toBe(customJobId);
    });

    it('should process jobs with custom jobIds', (done) => {
      const customJobId = 'customjob';
      queue.process('test', () => {
        return Promise.resolve();
      });

      queue.add('test', { foo: 'bar' }, { jobId: customJobId });

      queue.on('completed', (job) => {
        if (job.id === customJobId) {
          done();
        }
      });
    });
  });

  describe('.createBulk', () => {
    let jobs: ExtendedJob[];
    let inputJobs: { name: string; data: any; opts: any }[];

    beforeEach(async () => {
      inputJobs = [
        {
          name: 'jobA',
          data: {
            foo: 'bar'
          },
          opts: {
            testOpt: 'enabled'
          }
        },
        {
          name: 'jobB',
          data: {
            foo: 'baz'
          },
          opts: {
            testOpt: 'disabled'
          }
        }
      ];

      jobs = await (Bull as any).Job.createBulk(queue as any, inputJobs);
    });

    it('returns a promise for the jobs', () => {
      expect(jobs).toHaveLength(2);

      expect(jobs[0]).toHaveProperty('id');
      expect(jobs[0]).toHaveProperty('data');
    });

    it('should not modify input options', () => {
      expect(inputJobs[0].opts).not.toHaveProperty('jobId');
    });

    it('saves the first job in redis', async () => {
      const storedJob = await (Bull as any).Job.fromId(queue as any, jobs[0].id);
      expect(storedJob).toHaveProperty('id');
      expect(storedJob).toHaveProperty('data');

      expect(storedJob.data.foo).toBe('bar');
      expect(storedJob.opts).toBeInstanceOf(Object);
      expect(storedJob.opts.testOpt).toBe('enabled');
    });

    it('saves the second job in redis', async () => {
      const storedJob = await (Bull as any).Job.fromId(queue as any, jobs[1].id);
      expect(storedJob).toHaveProperty('id');
      expect(storedJob).toHaveProperty('data');

      expect(storedJob.data.foo).toBe('baz');
      expect(storedJob.opts).toBeInstanceOf(Object);
      expect(storedJob.opts.testOpt).toBe('disabled');
    });
  });

  describe('.add jobs on priority queues', () => {
    it('add 4 jobs with different priorities', async () => {
      await queue.add('test', { foo: 'bar' }, { jobId: '1', priority: 3 });
      await queue.add('test', { foo: 'bar' }, { jobId: '2', priority: 3 });
      await queue.add('test', { foo: 'bar' }, { jobId: '3', priority: 2 });
      await queue.add('test', { foo: 'bar' }, { jobId: '4', priority: 1 });

      const result = await queue.getWaiting();
      const waitingIDs = result.map(job => job.id);
      expect(waitingIDs).toHaveLength(4);
      expect(waitingIDs).toEqual(['4', '3', '1', '2']);
    });
  });

  describe('.update', () => {
    it('should allow updating job data', async () => {
      const job = await (Bull as any).Job.create(queue as any, { foo: 'bar' });
      
      await job.update({ baz: 'qux' });
      expect(job.data).toEqual({ baz: 'qux' });

      const storedJob = await (Bull as any).Job.fromId(queue as any, job.id);
      expect(storedJob.data).toEqual({ baz: 'qux' });
    });

    describe('when job was removed', () => {
      it('throws an error', async () => {
        const job = await (Bull as any).Job.create(queue as any, { foo: 'bar' });
        await job.remove();
        
        await expect(job.update({ baz: 'qux' })).rejects.toThrow('Missing key for job 1 updateData');
      });
    });
  });

  describe('.remove', () => {
    it('removes the job from redis', async () => {
      const job = await (Bull as any).Job.create(queue as any, { foo: 'bar' });
      await job.remove();
      const storedJob = await (Bull as any).Job.fromId(queue as any, job.id);
      expect(storedJob).toBeNull();
    });

    it('fails to remove a locked job', async () => {
      const job = await (Bull as any).Job.create(queue as any, { foo: 'bar' });
      await job.takeLock();
      const storedJob = await (Bull as any).Job.fromId(queue as any, job.id);
      await expect(storedJob.remove()).rejects.toThrow();
    });

    it('removes any job from active set', async () => {
      const job = await queue.add('test', { foo: 'bar' });
      await queue.getNextJob();
      
      const isActive = await job.isActive();
      expect(isActive).toBe(true);
      
      await job.releaseLock();
      await job.remove();
      
      const stored = await (Bull as any).Job.fromId(queue as any, job.id);
      expect(stored).toBeNull();
      
      const state = await job.getState();
      expect(state).toBe('stuck');
    });

    it('emits removed event', (done) => {
      queue.once('removed', (job) => {
        expect(job.data.foo).toBe('bar');
        done();
      });
      
      (Bull as any).Job.create(queue as any, { foo: 'bar' }).then(job => {
        job.remove();
      });
    });

    it('a successful job should be removable', (done) => {
      queue.process('test', () => {
        return Promise.resolve();
      });

      queue.add('test', { foo: 'bar' });

      queue.on('completed', (job) => {
        job.remove()
          .then(done)
          .catch(done);
      });
    });

    it('a failed job should be removable', (done) => {
      queue.process('test', () => {
        throw new Error();
      });

      queue.add('test', { foo: 'bar' });

      queue.on('failed', (job) => {
        job.remove()
          .then(done)
          .catch(done);
      });
    });
  });

  describe('.removeFromPattern', () => {
    it('remove jobs matching pattern', async () => {
      const jobIds = ['foo', 'foo1', 'foo2', 'foo3', 'foo4', 'bar', 'baz'];
      await Promise.all(
        jobIds.map(jobId => (Bull as any).Job.create(queue as any, { foo: 'bar' }, { jobId }))
      );

      await queue.removeJobs('foo*');

      for (let i = 0; i < jobIds.length; i++) {
        const storedJob = await (Bull as any).Job.fromId(queue as any, jobIds[i]);
        if (jobIds[i].startsWith('foo')) {
          expect(storedJob).toBeNull();
        } else {
          expect(storedJob).not.toBeNull();
        }
      }
    });
  });

  describe('.progress', () => {
    it('can set and get progress as number', async () => {
      const job = await (Bull as any).Job.create(queue as any, { foo: 'bar' });
      await job.progress(42);
      const storedJob = await (Bull as any).Job.fromId(queue as any, job.id);
      expect(await storedJob.progress()).toBe(42);
    });

    it('can set and get progress as object', async () => {
      const job = await (Bull as any).Job.create(queue as any, { foo: 'bar' });
      await job.progress({ total: 120, completed: 40 });
      const storedJob = await (Bull as any).Job.fromId(queue as any, job.id);
      expect(await storedJob.progress()).toEqual({ total: 120, completed: 40 });
    });

    describe('when job was removed', () => {
      it('throws an error', async () => {
        const job = await (Bull as any).Job.create(queue as any, { foo: 'bar' });
        await job.remove();
        await expect(job.progress({ total: 120, completed: 40 })).rejects.toThrow(
          'Missing key for job 1 updateProgress'
        );
      });
    });
  });

  describe('.log', () => {
    it('can log two rows with text', async () => {
      const firstLog = 'some log text 1';
      const secondLog = 'some log text 2';
      const job = await (Bull as any).Job.create(queue as any, { foo: 'bar' });

      await job.log(firstLog);
      await job.log(secondLog);

      let logs = await queue.getJobLogs(job.id);
      expect(logs).toEqual({ logs: [firstLog, secondLog], count: 2 });

      logs = await queue.getJobLogs(job.id, 0, 1);
      expect(logs).toEqual({ logs: [firstLog, secondLog], count: 2 });

      logs = await queue.getJobLogs(job.id, 0, 4000);
      expect(logs).toEqual({ logs: [firstLog, secondLog], count: 2 });

      logs = await queue.getJobLogs(job.id, 1, 1);
      expect(logs).toEqual({ logs: [secondLog], count: 2 });

      logs = await queue.getJobLogs(job.id, 0, 1, false);
      expect(logs).toEqual({ logs: [secondLog, firstLog], count: 2 });

      logs = await queue.getJobLogs(job.id, 0, 4000, false);
      expect(logs).toEqual({ logs: [secondLog, firstLog], count: 2 });

      logs = await queue.getJobLogs(job.id, 1, 1, false);
      expect(logs).toEqual({ logs: [firstLog], count: 2 });

      await job.remove();
      logs = await queue.getJobLogs(job.id);
      expect(logs).toEqual({ logs: [], count: 0 });
    });

    describe('when job was removed', () => {
      it('throws an error', async () => {
        const job = await (Bull as any).Job.create(queue as any, { foo: 'bar' });
        await job.remove();
        await expect(job.log('some log text 1')).rejects.toThrow(
          'Missing key for job 1 addLog'
        );
      });
    });
  });

  describe('.retry', () => {
    it('emits waiting event', (done) => {
      queue.add('test', { foo: 'bar' });
      queue.process('test', (_job, jobDone) => {
        jobDone && jobDone(new Error('the job failed'));
      });

      queue.once('failed', (job) => {
        queue.once('global:waiting', async (jobId) => {
          const job2 = await (Bull as any).Job.fromId(queue as any, jobId);
          expect(job2.data.foo).toBe('bar');
          done();
        });
        queue.once('registered:global:waiting', () => {
          job.retry();
        });
      });
    });

    it('sets retriedOn to a timestamp', (done) => {
      queue.add('test', { foo: 'bar' });
      queue.process('test', (_job, jobDone) => {
        jobDone && jobDone(new Error('the job failed'));
      });

      queue.once('failed', (job) => {
        queue.once('global:waiting', async (jobId) => {
          const now = Date.now();
          expect(job.retriedOn).toBeGreaterThanOrEqual(now - 1000);
          expect(job.retriedOn).toBeLessThanOrEqual(now);

          const job2 = await (Bull as any).Job.fromId(queue as any, jobId);
          expect(job2.retriedOn).toBeGreaterThanOrEqual(now - 1000);
          expect(job2.retriedOn).toBeLessThanOrEqual(now);
          done();
        });
        queue.once('registered:global:waiting', () => {
          job.retry();
        });
      });
    });
  });

  describe('.moveToCompleted', () => {
    it('marks the job as completed and returns new job', async () => {
      const job1 = await (Bull as any).Job.create(queue as any, { foo: 'bar' });
      const job2 = await (Bull as any).Job.create(queue as any, { foo: 'bar' }, { lifo: true });

      const isCompleted = await job2.isCompleted();
      expect(isCompleted).toBe(false);

      await queue.moveToActive();
      const result = await job2.moveToCompleted('succeeded', true);
      
      const isCompletedAfter = await job2.isCompleted();
      expect(isCompletedAfter).toBe(true);
      expect(job2.returnvalue).toBe('succeeded');
      expect(result[1]).toBe(job1.id);
    });
  });

  describe('.moveToFailed', () => {
    it('marks the job as failed', async () => {
      const job = await (Bull as any).Job.create(queue as any, { foo: 'bar' });
      
      const isFailed = await job.isFailed();
      expect(isFailed).toBe(false);

      await queue.moveToActive();
      await job.moveToFailed(new Error('test error'), true);

      const isFailedAfter = await job.isFailed();
      expect(isFailedAfter).toBe(true);
      expect(job.stacktrace).not.toBeNull();
      expect(job.stacktrace).toHaveLength(1);
    });

    it('moves the job to wait for retry if attempts are given', async () => {
      const job = await (Bull as any).Job.create(queue as any, { foo: 'bar' }, { attempts: 3 });
      
      const isFailed = await job.isFailed();
      expect(isFailed).toBe(false);

      await queue.moveToActive();
      await job.moveToFailed(new Error('test error'), true);

      const isFailedAfter = await job.isFailed();
      expect(isFailedAfter).toBe(false);
      expect(job.stacktrace).not.toBeNull();
      expect(job.stacktrace).toHaveLength(1);

      const isWaiting = await job.isWaiting();
      expect(isWaiting).toBe(true);
    });

    it('unlocks the job when moving it to delayed', (done) => {
      queue.process('test', () => {
        throw new Error('Oh dear');
      });

      queue.add('test', { foo: 'bar' }, { attempts: 3, backoff: 100 });

      queue.once('failed', () => {
        const client = new Redis();
        client.get(queue.toKey('lock')).then(lockValue => {
          expect(lockValue).toBeNull();
          done();
        });
      });
    });

    it('marks the job as failed when attempts made equal to attempts given', async () => {
      const job = await (Bull as any).Job.create(queue as any, { foo: 'bar' }, { attempts: 1 });
      
      const isFailed = await job.isFailed();
      expect(isFailed).toBe(false);

      await queue.moveToActive();
      await job.moveToFailed(new Error('test error'), true);

      const isFailedAfter = await job.isFailed();
      expect(isFailedAfter).toBe(true);
      expect(job.stacktrace).not.toBeNull();
      expect(job.stacktrace).toHaveLength(1);
    });

    it('moves the job to delayed for retry if attempts are given and backoff is non zero', async () => {
      const job = await (Bull as any).Job.create(queue as any, { foo: 'bar' }, { attempts: 3, backoff: 300 });
      
      const isFailed = await job.isFailed();
      expect(isFailed).toBe(false);

      await queue.moveToActive();
      await job.moveToFailed(new Error('test error'), true);

      const isFailedAfter = await job.isFailed();
      expect(isFailedAfter).toBe(false);
      expect(job.stacktrace).not.toBeNull();
      expect(job.stacktrace).toHaveLength(1);

      const isDelayed = await job.isDelayed();
      expect(isDelayed).toBe(true);
    });

    it('applies stacktrace limit on failure', async () => {
      const stackTraceLimit = 1;
      const job = await (Bull as any).Job.create(queue as any, { foo: 'bar' }, { stackTraceLimit, attempts: 2 });
      
      const isFailed = await job.isFailed();
      expect(isFailed).toBe(false);

      await queue.moveToActive();
      await job.moveToFailed(new Error('test error'), true);
      await queue.moveToActive();
      await job.moveToFailed(new Error('test error'), true);

      const isFailedAfter = await job.isFailed();
      expect(isFailedAfter).toBe(true);
      expect(job.stacktrace).not.toBeNull();
      expect(job.stacktrace).toHaveLength(stackTraceLimit);
    });
  });

  describe('.finished', () => {
    it('should resolve when the job has been completed', (done) => {
      queue.process('test', () => {
        return delay(500);
      });

      queue.add('test', { foo: 'bar' })
        .then(job => job.finished())
        .then(done)
        .catch(done);
    });

    it('should resolve when the job has been completed and return object', (done) => {
      queue.process('test', () => {
        return delay(500).then(() => ({ resultFoo: 'bar' }));
      });

      queue.add('test', { foo: 'bar' })
        .then(job => job.finished())
        .then(jobResult => {
          expect(jobResult).toBeInstanceOf(Object);
          expect(jobResult.resultFoo).toBe('bar');
          done();
        });
    });

    it('should resolve when the job has been delayed and completed and return object', (done) => {
      queue.process('test', () => {
        return delay(300).then(() => ({ resultFoo: 'bar' }));
      });

      queue.add('test', { foo: 'bar' })
        .then(job => delay(600).then(() => job.finished()))
        .then(jobResult => {
          expect(jobResult).toBeInstanceOf(Object);
          expect(jobResult.resultFoo).toBe('bar');
          done();
        });
    });

    it('should resolve when the job has been completed and return string', (done) => {
      queue.process('test', () => {
        return delay(500).then(() => 'a string');
      });

      queue.add('test', { foo: 'bar' })
        .then(job => delay(600).then(() => job.finished()))
        .then(jobResult => {
          expect(typeof jobResult).toBe('string');
          expect(jobResult).toBe('a string');
          done();
        });
    });

    it('should resolve when the job has been delayed and completed and return string', (done) => {
      queue.process('test', () => {
        return delay(300).then(() => 'a string');
      });

      queue.add('test', { foo: 'bar' })
        .then(job => job.finished())
        .then(jobResult => {
          expect(typeof jobResult).toBe('string');
          expect(jobResult).toBe('a string');
          done();
        });
    });

    it('should reject when the job has been failed', (done) => {
      queue.process('test', () => {
        return delay(500).then(() => {
          throw new Error('test error');
        });
      });

      queue.add('test', { foo: 'bar' })
        .then(job => job.finished())
        .then(() => {
          done(new Error('should have been rejected'));
        })
        .catch(err => {
          expect(err.message).toBe('test error');
          done();
        });
    });

    it('should resolve directly if already processed', (done) => {
      queue.process('test', () => {
        return Promise.resolve();
      });

      queue.add('test', { foo: 'bar' })
        .then(job => delay(500).then(() => job.finished()))
        .then(() => done())
        .catch(done);
    });

    it('should reject directly if already processed', (done) => {
      queue.process('test', () => {
        return Promise.reject(new Error('test error'));
      });

      queue.add('test', { foo: 'bar' })
        .then(job => delay(500).then(() => job.finished()))
        .then(() => {
          done(new Error('should have been rejected'));
        })
        .catch(err => {
          expect(err.message).toBe('test error');
          done();
        });
    });
  });

  describe('Locking', () => {
    let job: ExtendedJob;

    beforeEach(async () => {
      job = await (Bull as any).Job.create(queue as any, { foo: 'bar' });
    });

    it('can take a lock', async () => {
      const lockTaken = await job.takeLock();
      expect(lockTaken).toBeTruthy();

      const lockReleased = await job.releaseLock();
      expect(lockReleased).toBeUndefined();
    });

    it('take an already taken lock', async () => {
      const lockTaken1 = await job.takeLock();
      expect(lockTaken1).toBeTruthy();

      const lockTaken2 = await job.takeLock();
      expect(lockTaken2).toBeTruthy();
    });

    it('can release a lock', async () => {
      const lockTaken = await job.takeLock();
      expect(lockTaken).toBeTruthy();

      const lockReleased = await job.releaseLock();
      expect(lockReleased).toBeUndefined();
    });
  });

  describe('.fromJSON', () => {
    let data: { foo: string };

    beforeEach(() => {
      data = { foo: 'bar' };
    });

    it('should parse JSON data by default', async () => {
      const job = await (Bull as any).Job.create(queue as any, data, {});
      const jobParsed = (Bull as any).Job.fromJSON(queue as any, job.toData());
      expect(jobParsed.data).toEqual(data);
    });

    it('should not parse JSON data if "preventParsingData" option is specified', async () => {
      const job = await (Bull as any).Job.create(queue as any, data, { preventParsingData: true });
      const jobParsed = (Bull as any).Job.fromJSON(queue as any, job.toData());
      const expectedData = JSON.stringify(data);
      expect(jobParsed.data).toBe(expectedData);
    });
  });
});
