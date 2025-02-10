import { Redis } from "../../ioredis-sqlite";
import { buildQueue, Queue, cleanupQueues, JobStatus } from "./utils";
import delay from "delay";
import sinon from "sinon";

describe(".pause", () => {
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

  describe("globally", () => {
    it("should pause a queue until resumed", async () => {
      let ispaused = false;
      let counter = 2;

      const queue = await buildQueue();
      const resultPromise = new Promise<void>((resolve) => {
        queue.process("test", (job, jobDone = () => {}) => {
          expect(ispaused).toBe(false);
          expect(job.data.foo).toBe("paused");
          jobDone();
          counter--;
          if (counter === 0) {
            resolve(queue.close());
          }
        });
      });

      await Promise.all([
        (async () => {
          await queue.pause();
          ispaused = true;
          const paused = await queue.isPaused();
          expect(paused).toBe(true);
          await queue.add({ foo: "paused" } as any);
          await queue.add({ foo: "paused" } as any);
          ispaused = false;
          await queue.resume();
          const stillPaused = await queue.isPaused();
          expect(stillPaused).toBe(false);
        })(),
        resultPromise,
      ] as const);
    });

    it("should be able to pause a running queue and emit relevant events", async () => {
      let ispaused = false;
      let isresumed = true;
      let first = true;

      const queue = await buildQueue();

      await new Promise<void>((resolve, reject) => {
        queue.process("test", async (job) => {
          try {
            expect(ispaused).toBe(false);
            expect(job.data.foo).toBe("paused");

            if (first) {
              first = false;
              ispaused = true;
              await queue.pause();
            } else {
              expect(isresumed).toBe(true);
              await queue.close();
              resolve();
            }
          } catch (err) {
            reject(err);
          }
        });

        queue.add({ foo: "paused" } as any);
        queue.add({ foo: "paused" } as any);

        queue.on("paused", () => {
          ispaused = false;
          queue.resume().catch(() => {
            // Swallow error.
          });
        });

        queue.on("resumed", () => {
          isresumed = true;
        });
      });
    });

    it("should not processed delayed jobs", async () => {
      jest.setTimeout(5000);
      const queue = await buildQueue("pause-test");

      await new Promise<void>(async (resolve, reject) => {
        queue.process("test", () => {
          reject(new Error("should not process delayed jobs in paused queue."));
        });

        try {
          await queue.pause();
          await queue.add({} as any, { delay: 500 });
          const counts = await queue.getJobCounts();
          expect(counts).toHaveProperty("paused", 0);
          expect(counts).toHaveProperty("waiting", 0);
          expect(counts).toHaveProperty("delayed", 1);
          await delay(1000);
          const newCounts = await queue.getJobCounts();
          expect(newCounts).toHaveProperty("paused", 1);
          expect(newCounts).toHaveProperty("waiting", 0);
          resolve();
        } catch (err) {
          reject(err);
        }
      });
    });
  });

  describe("locally", () => {
    it("should pause the queue locally", async () => {
      let counter = 2;
      const queue = buildQueue();

      await queue.pause(true /* Local */);
      const paused = await queue.isPaused(true);
      expect(paused).toBe(true);

      await new Promise<void>((resolve, reject) => {
        queue.process("test", (job, jobDone = () => {}) => {
          try {
            expect(queue.paused).toBeFalsy();
            jobDone();
            counter--;
            if (counter === 0) {
              queue.close().then(() => resolve());
            }
          } catch (err) {
            reject(err);
          }
        });

        queue
          .add({ foo: "paused" } as any)
          .then(() => queue.add({ foo: "paused" } as any))
          .then(async () => {
            expect(counter).toBe(2);
            expect(queue.paused).toBeTruthy();
            await queue.resume(true /* Local */);
            const stillPaused = await queue.isPaused(true);
            expect(stillPaused).toBe(false);
          })
          .catch(reject);
      });
    });

    it("should wait until active jobs are finished before resolving pause", async () => {
      const queue = buildQueue();
      const startProcessing = new Promise<void>((resolve) => {
        queue.process("test", () => {
          resolve();
          return delay(200);
        });
      });

      await queue.isReady();
      const jobs = await Promise.all([
        ...Array(10)
          .fill(0)
          .map((_, i) => queue.add(i) as Promise<any>),
        startProcessing,
      ] as const);

      await queue.pause(true);
      const active = await queue.getJobCountByTypes(["active" as JobStatus]);
      expect(active).toBe(0);
      expect(queue.paused).toBeTruthy();

      const paused = await queue.getJobCountByTypes([
        "delayed" as JobStatus,
        "wait" as JobStatus,
      ]);
      expect(paused).toBe(9);

      await queue.add({} as any);
      const newActive = await queue.getJobCountByTypes(["active" as JobStatus]);
      expect(newActive).toBe(0);

      const newPaused = await queue.getJobCountByTypes([
        "paused" as JobStatus,
        "wait" as JobStatus,
        "delayed" as JobStatus,
      ]);
      expect(newPaused).toBe(10);

      await queue.close();
    });

    it("should pause the queue locally when more than one worker is active", async () => {
      const queue1 = buildQueue("pause-queue");
      const queue2 = buildQueue("pause-queue");

      const [queue1IsProcessing, queue2IsProcessing] = await Promise.all([
        new Promise<void>((resolve) => {
          queue1.process("test", (job, jobDone = () => {}) => {
            resolve();
            setTimeout(() => jobDone(), 200);
          });
        }),
        new Promise<void>((resolve) => {
          queue2.process("test", (job, jobDone = () => {}) => {
            resolve();
            setTimeout(() => jobDone(), 200);
          });
        }),
      ] as const);

      await queue1.add(1);
      await queue1.add(2);
      await queue1.add(3);
      await queue1.add(4);

      await Promise.all<void>([queue1IsProcessing, queue2IsProcessing]);
      await Promise.all<void>([queue1.pause(true), queue2.pause(true)]);

      const [active, pending, completed] = (await Promise.all([
        queue1.getJobCountByTypes(["active" as JobStatus]),
        queue1.getJobCountByTypes(["wait" as JobStatus]),
        queue1.getJobCountByTypes(["completed" as JobStatus]),
      ])) as [number, number, number];

      expect(active).toBe(0);
      expect(pending).toBe(2);
      expect(completed).toBe(2);

      await Promise.all<void>([queue1.close(), queue2.close()]);
    });

    it("should wait for blocking job retrieval to complete before pausing", async () => {
      const queue = buildQueue();
      const startsProcessing = new Promise<void>((resolve) => {
        queue.process("test", () => {
          resolve();
          return delay(200);
        });
      });

      await queue.add(1);
      await startsProcessing;
      await queue.pause(true);
      await queue.add(2);

      const [active, pending, completed] = (await Promise.all([
        queue.getJobCountByTypes(["active" as JobStatus]),
        queue.getJobCountByTypes(["wait" as JobStatus]),
        queue.getJobCountByTypes(["completed" as JobStatus]),
      ])) as [number, number, number];

      expect(active).toBe(0);
      expect(pending).toBe(1);
      expect(completed).toBe(1);

      await queue.close();
    });

    it("should not initialize blocking client if not already initialized", async () => {
      const createClient = sinon.spy(() => client);
      const queue = buildQueue("pause-queue", { createClient });

      await queue.pause(true);
      const bClientCalls = createClient
        .getCalls()
        .filter((c) => (c.args as any)[0] === "bclient");
      expect(bClientCalls).toHaveLength(0);
    });

    it("pauses fast when queue is drained", async () => {
      jest.setTimeout(10000);
      const queue = buildQueue("test");

      await new Promise<void>(async (resolve, reject) => {
        queue.process("test", () => Promise.resolve());
        await queue.add({} as any);

        queue.on("drained", async () => {
          try {
            await delay(500);
            const start = new Date().getTime();
            await queue.pause(true);
            const finish = new Date().getTime();
            expect(finish - start).toBeLessThan(1000);
            await queue.close();
            resolve();
          } catch (err) {
            reject(err);
          }
        });
      });
    });

    describe("with doNotWaitActive=true", () => {
      it("should not wait for active jobs to finish", async () => {
        const queue = buildQueue();
        await queue.add({} as any);

        let finishJob: () => void;

        await new Promise<void>((resolve) => {
          queue.process("test", () => {
            resolve();
            return new Promise<void>((innerResolve) => {
              finishJob = innerResolve;
            });
          });
        });

        await queue.pause(true, true);
        finishJob!();
      });

      it("should not process new jobs", async () => {
        const queue = buildQueue();
        const nextJobPromise = queue.getNextJob();
        await queue.pause(true, true);
        await queue.add({} as any);
        const nextJob = await nextJobPromise;
        expect(nextJob).toBeUndefined();
      });

      it("should not initialize blocking client if not already initialized", async () => {
        const createClient = sinon.spy(() => client);
        const queue = buildQueue("pause-queue", { createClient });

        await queue.pause(true, true);
        const bClientCalls = createClient
          .getCalls()
          .filter((c) => (c.args as any)[0] === "bclient");
        expect(bClientCalls).toHaveLength(0);
      });
    });
  });
});
