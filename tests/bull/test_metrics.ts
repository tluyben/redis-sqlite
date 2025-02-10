import { Redis } from '../../ioredis-sqlite';
import { buildQueue, Queue } from './utils';
import sinon from 'sinon';

const ONE_SECOND = 1000;
const ONE_MINUTE = 60 * ONE_SECOND;
const ONE_HOUR = 60 * ONE_MINUTE;

// TODO: Import from bull once types are available
const MetricsTime = {
  ONE_HOUR: 60,
  FIFTEEN_MINUTES: 15
};

describe('metrics', () => {
  let clock: sinon.SinonFakeTimers;

  beforeEach(async () => {
    clock = sinon.useFakeTimers();
    const client = new Redis();
    await client.flushdb();
    await client.quit();
  });

  it('should gather metrics for completed jobs', async () => {
    const date = new Date('2017-02-07 9:24:00');
    clock.setSystemTime(date);
    clock.tick(0);

    const timmings = [
      0,
      0, // For the fixtures to work we need to use 0 as first timing
      ONE_MINUTE / 2,
      ONE_MINUTE / 2,
      0,
      0,
      ONE_MINUTE,
      ONE_MINUTE,
      ONE_MINUTE * 3,
      ONE_SECOND * 70,
      ONE_SECOND * 50,
      ONE_HOUR,
      ONE_MINUTE
    ];

    const fixture = [
      1,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      1,
      1,
      1,
      0,
      0,
      1,
      1,
      3,
      3
    ];

    const numJobs = timmings.length;

    const queue = buildQueue('metrics', {
      metrics: {
        maxDataPoints: MetricsTime.ONE_HOUR * 2
      }
    });

    queue.process('test', job => {
      clock.tick(timmings[job.data.index]);
    });

    let processed = 0;
    const completing = new Promise<void>(resolve => {
      queue.on('completed', async () => {
        processed++;
        if (processed === numJobs) {
          resolve();
        }
      });
    });

    for (let i = 0; i < numJobs; i++) {
      await queue.add('test', { index: i });
    }

    await completing;

    const metrics = await queue.getMetrics('completed');

    const numPoints = Math.floor(
      timmings.reduce((sum, timing) => sum + timing, 0) / ONE_MINUTE
    );

    expect(metrics.meta.count).toBe(numJobs);
    expect(metrics.data.length).toBe(numPoints);
    expect(metrics.count).toBe(metrics.data.length);
    expect(processed).toBe(numJobs);
    expect(metrics.data).toEqual(fixture);

    clock.restore();
    await queue.close();
  });

  it('should only keep metrics for "maxDataPoints"', async () => {
    const date = new Date('2017-02-07 9:24:00');
    clock.setSystemTime(date);
    clock.tick(0);

    const timmings = [
      0, // For the fixtures to work we need to use 0 as first timing
      0,
      ONE_MINUTE / 2,
      ONE_MINUTE / 2,
      0,
      0,
      ONE_MINUTE,
      ONE_MINUTE,
      ONE_MINUTE * 3,
      ONE_HOUR,
      0,
      0,
      ONE_MINUTE,
      ONE_MINUTE
    ];

    const fixture = [
      1,
      3,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0
    ];

    const numJobs = timmings.length;

    const queue = buildQueue('metrics', {
      metrics: {
        maxDataPoints: MetricsTime.FIFTEEN_MINUTES
      }
    });

    queue.process('test', job => {
      clock.tick(timmings[job.data.index]);
    });

    let processed = 0;
    const completing = new Promise<void>(resolve => {
      queue.on('completed', async () => {
        processed++;
        if (processed === numJobs) {
          resolve();
        }
      });
    });

    for (let i = 0; i < numJobs; i++) {
      await queue.add('test', { index: i });
    }

    await completing;

    const metrics = await queue.getMetrics('completed');

    expect(metrics.meta.count).toBe(numJobs);
    expect(metrics.data.length).toBe(MetricsTime.FIFTEEN_MINUTES);
    expect(metrics.count).toBe(metrics.data.length);
    expect(processed).toBe(numJobs);
    expect(metrics.data).toEqual(fixture);

    clock.restore();
    await queue.close();
  });

  it('should gather metrics for failed jobs', async () => {
    const date = new Date('2017-02-07 9:24:00');
    clock.setSystemTime(date);
    clock.tick(0);

    const timmings = [
      0, // For the fixtures to work we need to use 0 as first timing
      ONE_MINUTE,
      ONE_MINUTE / 5,
      ONE_MINUTE / 2,
      0,
      ONE_MINUTE,
      ONE_MINUTE * 3,
      0
    ];

    const fixture = [0, 0, 1, 4, 1];

    const numJobs = timmings.length;

    const queue = buildQueue('metrics', {
      metrics: {
        maxDataPoints: MetricsTime.ONE_HOUR * 2
      }
    });

    queue.process('test', async job => {
      clock.tick(timmings[job.data.index]);
      throw new Error('test');
    });

    let processed = 0;
    const completing = new Promise<void>(resolve => {
      queue.on('failed', async () => {
        processed++;
        if (processed === numJobs) {
          resolve();
        }
      });
    });

    for (let i = 0; i < numJobs; i++) {
      await queue.add('test', { index: i });
    }

    await completing;

    const metrics = await queue.getMetrics('failed');

    const numPoints = Math.floor(
      timmings.reduce((sum, timing) => sum + timing, 0) / ONE_MINUTE
    );

    expect(metrics.meta.count).toBe(numJobs);
    expect(metrics.data.length).toBe(numPoints);
    expect(metrics.count).toBe(metrics.data.length);
    expect(processed).toBe(numJobs);
    expect(metrics.data).toEqual(fixture);

    clock.restore();
    await queue.close();
  });

  it('should get metrics with pagination', async () => {
    const date = new Date('2017-02-07 9:24:00');
    clock.setSystemTime(date);
    clock.tick(0);

    const timmings = [
      0,
      0, // For the fixtures to work we need to use 0 as first timing
      ONE_MINUTE / 2,
      ONE_MINUTE / 2,
      0,
      0,
      ONE_MINUTE,
      ONE_MINUTE,
      ONE_MINUTE * 3,
      ONE_HOUR,
      ONE_MINUTE
    ];

    const numJobs = timmings.length;

    const queue = buildQueue('metrics', {
      metrics: {
        maxDataPoints: MetricsTime.ONE_HOUR * 2
      }
    });

    queue.process('test', async job => {
      clock.tick(timmings[job.data.index]);
    });

    let processed = 0;
    const completing = new Promise<void>(resolve => {
      queue.on('completed', async () => {
        processed++;
        if (processed === numJobs) {
          resolve();
        }
      });
    });

    for (let i = 0; i < numJobs; i++) {
      await queue.add('test', { index: i });
    }

    await completing;

    expect(processed).toBe(numJobs);

    const numPoints = Math.floor(
      timmings.reduce((sum, timing) => sum + timing, 0) / ONE_MINUTE
    );

    const pageSize = 10;
    const data: number[] = [];
    let skip = 0;

    while (skip < numPoints) {
      const metrics = await queue.getMetrics(
        'completed',
        skip,
        skip + pageSize - 1
      );
      expect(metrics.meta.count).toBe(numJobs);
      expect(metrics.data.length).toBe(
        Math.min(numPoints - skip, pageSize)
      );

      data.push(...metrics.data);
      skip += pageSize;
    }

    const metrics = await queue.getMetrics('completed');
    expect(data).toEqual(metrics.data);

    clock.restore();
    await queue.close();
  });
});
