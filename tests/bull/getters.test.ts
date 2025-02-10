import { Redis } from '../../ioredis-sqlite';
import { buildQueue, Queue, ExtendedJob } from './utils';
import _ from 'lodash';

describe('Jobs getters', () => {
  jest.setTimeout(12000);
  let queue: Queue;
  let client: Redis;

  beforeEach(async () => {
    client = new Redis();
    await client.flushdb();
  });

  beforeEach(() => {
    queue = buildQueue();
  });

  afterEach(async () => {
    jest.setTimeout(queue.settings.stalledInterval * (1 + queue.settings.maxStalledCount));
    await queue.clean(1000);
    await queue.close();
    await client.quit();
  });

  it('should get waiting jobs', async () => {
    await Promise.all([
      queue.add('test', { foo: 'bar' }),
      queue.add('test', { baz: 'qux' })
    ]);

    const jobs = await queue.getWaiting();
    expect(jobs).toBeInstanceOf(Array);
    expect(jobs).toHaveLength(2);
    expect(jobs[0].data.foo).toBe('bar');
    expect(jobs[1].data.baz).toBe('qux');
  });

  it('should get paused jobs', async () => {
    await queue.pause();
    await Promise.all([
      queue.add('test', { foo: 'bar' }),
      queue.add('test', { baz: 'qux' })
    ]);

    const jobs = await queue.getWaiting();
    expect(jobs).toBeInstanceOf(Array);
    expect(jobs).toHaveLength(2);
    expect(jobs[0].data.foo).toBe('bar');
    expect(jobs[1].data.baz).toBe('qux');
  });

  it('should get active jobs', (done) => {
    queue.process('test', (job, jobDone) => {
      queue.getActive().then(jobs => {
        expect(jobs).toBeInstanceOf(Array);
        expect(jobs).toHaveLength(1);
        expect(jobs[0].data.foo).toBe('bar');
        done();
      });
      jobDone && jobDone();
    });

    queue.add('test', { foo: 'bar' });
  });

  it('should get a specific job', async () => {
    const data = { foo: 'sup!' };
    const job = await queue.add('test', data);
    const returnedJob = await queue.getJob(job.id);
    expect(returnedJob?.data).toEqual(data);
    expect(returnedJob?.id).toBe(job.id);
  });

  it('should get completed jobs', (done) => {
    let counter = 2;

    queue.process('test', (_job, jobDone) => {
      jobDone && jobDone();
    });

    queue.on('completed', () => {
      counter--;

      if (counter === 0) {
        queue.getCompleted().then(jobs => {
          expect(jobs).toBeInstanceOf(Array);
          done();
        });
      }
    });

    queue.add('test', { foo: 'bar' });
    queue.add('test', { baz: 'qux' });
  });

  it('should get completed jobs excluding their data', (done) => {
    let counter = 2;
    const timestamp = Date.now();

    queue.process('test', (_job, jobDone) => {
      jobDone && jobDone();
    });

    queue.on('completed', () => {
      counter--;

      if (counter === 0) {
        queue.getCompleted(0, -1, { excludeData: true }).then(jobs => {
          expect(jobs).toBeInstanceOf(Array);
          expect(jobs).toHaveLength(2);

          for (let i = 0; i < jobs.length; i++) {
            expect(jobs[i]).toHaveProperty('data');
            expect(jobs[i].data).toEqual({});

            expect(jobs[i]).toHaveProperty('timestamp');
            expect(jobs[i].timestamp).toBeGreaterThanOrEqual(timestamp);
            expect(jobs[i]).toHaveProperty('processedOn');
            expect(jobs[i].processedOn).toBeGreaterThanOrEqual(timestamp);
          }

          done();
        });
      }
    });

    queue.add('test', { foo: 'bar' });
    queue.add('test', { baz: 'qux' });
  });

  it('should get failed jobs', (done) => {
    let counter = 2;

    queue.process('test', (_job, jobDone) => {
      jobDone && jobDone(new Error('Forced error'));
    });

    queue.on('failed', () => {
      counter--;

      if (counter === 0) {
        queue.getFailed().then(jobs => {
          expect(jobs).toBeInstanceOf(Array);
          done();
        });
      }
    });

    queue.add('test', { foo: 'bar' });
    queue.add('test', { baz: 'qux' });
  });

  describe('.getCountsPerPriority', () => {
    it('returns job counts per priority', async () => {
      const jobsArray = Array.from(Array(42).keys()).map(index => ({
        name: 'test',
        data: {},
        opts: {
          priority: index % 4
        }
      }));
      await queue.addBulk(jobsArray);
      const counts = await queue.getCountsPerPriority([0, 1, 2, 3]);
      expect(counts).toEqual({
        '0': 11,
        '1': 11,
        '2': 10,
        '3': 10
      });
    });

    describe('when queue is paused', () => {
      it('returns job counts per priority', async () => {
        await queue.pause();
        const jobsArray = Array.from(Array(42).keys()).map(index => ({
          name: 'test',
          data: {},
          opts: {
            priority: index % 4
          }
        }));
        await queue.addBulk(jobsArray);
        const counts = await queue.getCountsPerPriority([0, 1, 2, 3]);
        
        expect(counts).toEqual({
          '0': 11,
          '1': 11,
          '2': 10,
          '3': 10
        });
      });
    });  
  });

  it('fails jobs that exceed their specified timeout', (done) => {
    queue.process('test', (_job, jobDone) => {
      setTimeout(() => jobDone && jobDone(), 200);
    });

    queue.on('failed', (_job, error) => {
      expect(error.message).toContain('timed out');
      done();
    });

    queue.on('completed', () => {
      done(new Error('The job should have timed out'));
    });

    queue.add(
      'test',
      { some: 'data' },
      {
        timeout: 100
      }
    );
  });

  it('should return all completed jobs when not setting start/end', (done) => {
    queue.process('test', (_job, completed) => {
      completed && completed();
    });

    queue.on(
      'completed',
      _.after(3, () => {
        queue
          .getJobs(['completed'])
          .then(jobs => {
            expect(jobs).toBeInstanceOf(Array);
            expect(jobs).toHaveLength(3);
            expect(jobs[0]).toHaveProperty('finishedOn');
            expect(jobs[1]).toHaveProperty('finishedOn');
            expect(jobs[2]).toHaveProperty('finishedOn');

            expect(jobs[0]).toHaveProperty('processedOn');
            expect(jobs[1]).toHaveProperty('processedOn');
            expect(jobs[2]).toHaveProperty('processedOn');
            done();
          })
          .catch(done);
      })
    );

    queue.add('test', { foo: 1 });
    queue.add('test', { foo: 2 });
    queue.add('test', { foo: 3 });
  });

  it('should return all failed jobs when not setting start/end', (done) => {
    queue.process('test', (_job, completed) => {
      completed && completed(new Error('error'));
    });

    queue.on(
      'failed',
      _.after(3, () => {
        queue
          .getJobs(['failed'])
          .then(jobs => {
            expect(jobs).toBeInstanceOf(Array);
            expect(jobs).toHaveLength(3);
            expect(jobs[0]).toHaveProperty('finishedOn');
            expect(jobs[1]).toHaveProperty('finishedOn');
            expect(jobs[2]).toHaveProperty('finishedOn');

            expect(jobs[0]).toHaveProperty('processedOn');
            expect(jobs[1]).toHaveProperty('processedOn');
            expect(jobs[2]).toHaveProperty('processedOn');
            done();
          })
          .catch(done);
      })
    );

    queue.add('test', { foo: 1 });
    queue.add('test', { foo: 2 });
    queue.add('test', { foo: 3 });
  });

  it('should return subset of jobs when setting positive range', (done) => {
    queue.process('test', (_job, completed) => {
      completed && completed();
    });

    queue.on(
      'completed',
      _.after(3, () => {
        queue
          .getJobs(['completed'], 1, 2, true)
          .then(jobs => {
            expect(jobs).toBeInstanceOf(Array);
            expect(jobs).toHaveLength(2);
            expect(jobs[0].data.foo).toBe(2);
            expect(jobs[1].data.foo).toBe(3);
            expect(jobs[0]).toHaveProperty('finishedOn');
            expect(jobs[1]).toHaveProperty('finishedOn');
            expect(jobs[0]).toHaveProperty('processedOn');
            expect(jobs[1]).toHaveProperty('processedOn');
            done();
          })
          .catch(done);
      })
    );

    queue
      .add('test', { foo: 1 })
      .then(() => queue.add('test', { foo: 2 }))
      .then(() => queue.add('test', { foo: 3 }));
  });

  it('should return subset of jobs when setting a negative range', (done) => {
    queue.process('test', (_job, completed) => {
      completed && completed();
    });

    queue.on(
      'completed',
      _.after(3, () => {
        queue
          .getJobs(['completed'], -3, -1, true)
          .then(jobs => {
            expect(jobs).toBeInstanceOf(Array);
            expect(jobs).toHaveLength(3);
            expect(jobs[0].data.foo).toBe(1);
            expect(jobs[1].data.foo).toBe(2);
            expect(jobs[2].data.foo).toBe(3);
            done();
          })
          .catch(done);
      })
    );

    queue.add('test', { foo: 1 });
    queue.add('test', { foo: 2 });
    queue.add('test', { foo: 3 });
  });

  it('should return subset of jobs when range overflows', (done) => {
    queue.process('test', (_job, completed) => {
      completed && completed();
    });

    queue.on(
      'completed',
      _.after(3, () => {
        queue
          .getJobs(['completed'], -300, 99999, true)
          .then(jobs => {
            expect(jobs).toBeInstanceOf(Array);
            expect(jobs).toHaveLength(3);
            expect(jobs[0].data.foo).toBe(1);
            expect(jobs[1].data.foo).toBe(2);
            expect(jobs[2].data.foo).toBe(3);
            done();
          })
          .catch(done);
      })
    );

    queue.add('test', { foo: 1 });
    queue.add('test', { foo: 2 });
    queue.add('test', { foo: 3 });
  });

  it('should return jobs for multiple types', (done) => {
    let counter = 0;
    queue.process('test', () => {
      counter++;
      if (counter == 2) {
        return queue.pause();
      }
    });

    queue.on(
      'completed',
      _.after(2, () => {
        queue
          .getJobs(['completed', 'paused'])
          .then(jobs => {
            expect(jobs).toBeInstanceOf(Array);
            expect(jobs).toHaveLength(3);
            done();
          })
          .catch(done);
      })
    );

    queue.add('test', { foo: 1 });
    queue.add('test', { foo: 2 });
    queue.add('test', { foo: 3 });
  });
});
