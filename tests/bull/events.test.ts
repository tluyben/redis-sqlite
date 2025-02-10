import { Redis } from "../../ioredis-sqlite";
import { buildQueue, Queue, simulateDisconnect, ExtendedJob } from "./utils";
import Bull from "bull";
import delay from "delay";

describe("events", () => {
  let queue: Queue;

  beforeEach(async () => {
    const client = new Redis();
    await client.flushdb();
    queue = buildQueue("test events", {
      settings: {
        stalledInterval: 100,
        lockDuration: 50,
      },
    });
    return queue;
  });

  afterEach(async () => {
    await queue.close();
  });

  it("should emit waiting when a job has been added", (done) => {
    queue.on("waiting", () => {
      done();
    });

    queue.on("registered:waiting", () => {
      queue.add("test", { foo: "bar" });
    });
  });

  it("should emit global:waiting when a job has been added", (done) => {
    queue.on("global:waiting", () => {
      done();
    });

    queue.on("registered:global:waiting", () => {
      queue.add("test", { foo: "bar" });
    });
  });

  it("should emit stalled when a job has been stalled", (done) => {
    queue.on("completed", () => {
      done(new Error("should not have completed"));
    });

    queue.process("test", () => {
      return delay(250);
    });

    queue.add("test", { foo: "bar" });

    const queue2 = buildQueue("test events", {
      settings: {
        stalledInterval: 100,
      },
    });

    queue2.on("stalled", () => {
      queue2.close().then(done);
    });

    queue.on("active", () => {
      queue2.startMoveUnlockedJobsToWait();
      queue.close(true);
    });
  });

  it("should emit global:stalled when a job has been stalled", (done) => {
    queue.on("completed", () => {
      done(new Error("should not have completed"));
    });

    queue.process("test", () => {
      return delay(250);
    });

    queue.add("test", { foo: "bar" });

    const queue2 = buildQueue("test events", {
      settings: {
        stalledInterval: 100,
      },
    });

    queue2.on("global:stalled", () => {
      queue2.close().then(done);
    });

    queue.on("active", () => {
      queue2.startMoveUnlockedJobsToWait();
      queue.close(true);
    });
  });

  it("should emit global:failed when a job has stalled more than allowable times", (done) => {
    queue.on("completed", () => {
      done(new Error("should not have completed"));
    });

    queue.process("test", () => {
      return delay(250);
    });

    queue.add("test", { foo: "bar" });

    const queue2 = buildQueue("test events", {
      settings: {
        stalledInterval: 100,
        maxStalledCount: 0,
      },
    });

    queue2.on("global:failed", (jobId: string, error: Error) => {
      expect(error.message).toBe("job stalled more than maxStalledCount");
      expect(jobId).toBeDefined();
      queue2.close().then(done);
    });

    queue.on("active", () => {
      queue2.startMoveUnlockedJobsToWait();
      queue.close(true);
    });
  });

  it("emits waiting event when a job is added", (done) => {
    const queue = buildQueue();

    queue.once("waiting", async (jobId) => {
      const job = await (Bull as any).Job.fromId(queue as any, jobId);
      expect(job.data.foo).toBe("bar");
      await queue.close();
      done();
    });

    queue.once("registered:waiting", () => {
      queue.add("test", { foo: "bar" });
    });
  });

  it("emits drained and global:drained event when all jobs have been processed", (done) => {
    const queue = buildQueue("event drained", {
      settings: { drainDelay: 1 },
    });

    queue.process("test", (_job, done) => {
      done && done();
    });

    let drainCount = 0;
    const drainedCallback = () => {
      drainCount++;
      if (drainCount === 2) {
        queue.getJobCountByTypes("completed").then((completed) => {
          expect(completed).toBe(2);
          queue.close().then(done);
        });
      }
    };

    queue.once("drained", drainedCallback);
    queue.once("global:drained", drainedCallback);

    queue.add("test", { foo: "bar" });
    queue.add("test", { foo: "baz" });
  });

  it("should emit an event when a new message is added to the queue", (done) => {
    const client = new Redis();
    const queue = new Bull("test pub sub");
    queue.on("waiting", (jobId: string | number) => {
      expect(parseInt(String(jobId), 10)).toBe(1);
      client.quit();
      done();
    });
    queue.once("registered:waiting", () => {
      queue.add("test", { test: "stuff" });
    });
  });

  it("should emit an event when a new priority message is added to the queue", (done) => {
    const client = new Redis();
    const queue = new Bull("test pub sub");
    queue.on("waiting", (jobId: string | number) => {
      expect(parseInt(String(jobId), 10)).toBe(1);
      client.quit();
      done();
    });
    queue.once("registered:waiting", () => {
      queue.add("test", { test: "stuff" }, { priority: 1 });
    });
  });

  it("should emit an event when a job becomes active", (done) => {
    const queue = buildQueue();
    queue.process("test", (_job, done) => {
      done && done();
    });
    queue.add("test", {});
    queue.once("active", () => {
      queue.once("completed", () => {
        queue.close().then(done);
      });
    });
  });

  it("should emit an event if a job fails to extend lock", (done) => {
    const LOCK_RENEW_TIME = 1;
    queue = buildQueue("queue fails to extend lock", {
      settings: {
        lockRenewTime: LOCK_RENEW_TIME,
      },
    });
    queue.once(
      "lock-extension-failed",
      (lockingFailedJob: ExtendedJob, error: Error) => {
        expect(lockingFailedJob.data.foo).toBe("lockingFailedJobFoo");
        expect(error.message).toBe("Connection is closed.");
        queue.close().then(done);
      }
    );
    queue.isReady().then(() => {
      queue.process("test", () => {
        simulateDisconnect(queue);
        return delay(LOCK_RENEW_TIME + 0.25);
      });
      queue.add("test", { foo: "lockingFailedJobFoo" });
    });
  });

  it("should listen to global events", (done) => {
    const queue1 = buildQueue();
    const queue2 = buildQueue();
    queue1.process("test", (_job, done) => {
      done && done();
    });

    let state: string | undefined;
    queue2.on("global:waiting", () => {
      expect(state).toBeUndefined();
      state = "waiting";
    });
    queue2.once("registered:global:waiting", () => {
      queue2.once("global:active", () => {
        expect(state).toBe("waiting");
        state = "active";
      });
    });
    queue2.once("registered:global:active", () => {
      queue2.once("global:completed", () => {
        expect(state).toBe("active");
        queue1.close().then(() => {
          queue2.close().then(done);
        });
      });
    });

    queue2.once("registered:global:completed", () => {
      queue1.add("test", {});
    });
  });
});
