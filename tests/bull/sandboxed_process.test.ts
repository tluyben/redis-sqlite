import { Redis } from "../../ioredis-sqlite";
import { Queue as BullQueue, Job, ProcessCallbackFunction } from "bull";
import path from "path";
import delay from "delay";
import pReflect from "p-reflect";
import { buildQueue } from "./utils";

// Extend Queue type to include internal properties used in tests
interface ChildPool {
  retained: { [key: string]: any };
  free: { [key: string]: any[] };
  getAllFree(): any[];
  clean(): Promise<void>;
  retain(processFile: string): Promise<any>;
  release(child: any): void;
}

interface QueueChild {
  killed?: boolean;
  finished?: boolean;
  exitCode: number | null;
}

// Extend Job type with additional methods
interface ExtendedJob<T = any> extends Omit<Job<T>, "progress"> {
  isDiscarded(): boolean;
  progress(): Promise<number>;
  finished(): Promise<any>;
}

// Custom Queue type that includes our extensions
interface Queue<T = any>
  extends Omit<BullQueue<T>, "process" | "add" | "on" | "once"> {
  childPool: ChildPool;
  client: any;
  eclient: any;
  process(processor: string): void;
  process(name: string, processor: string): void;
  process(concurrency: number, processor: string): void;
  getJobLogs(
    jobId: string | number,
    start?: number,
    end?: number,
    asc?: boolean
  ): Promise<{ logs: string[]; count: number }>;
  add(data?: T): Promise<ExtendedJob<T>>;
  add(name: string, data: T): Promise<ExtendedJob<T>>;
  on(
    event: "completed",
    callback: (job: ExtendedJob, result: any) => void
  ): this;
  on(event: "failed", callback: (job: ExtendedJob, error: Error) => void): this;
  on(
    event: "progress",
    callback: (job: ExtendedJob, progress: number) => void
  ): this;
  on(event: "error", callback: (error: Error) => void): this;
  once(event: "active", callback: (job: ExtendedJob) => void): this;
}

// Extend Job type with additional methods
interface ExtendedJob<T = any> extends Omit<Job<T>, "progress" | "finished"> {
  id: string | number;
  data: T;
  isDiscarded(): boolean;
  progress(): Promise<number>;
  finished(): Promise<any>;
  failedReason?: string;
}

// Type for pReflect result
interface ReflectResult<T = any> {
  isRejected: boolean;
  isFulfilled: boolean;
  reason?: Error & { message: string };
  value?: T;
}

// Cast pReflect to handle our custom type
const typedReflect = pReflect as <T>(
  promise: Promise<T>
) => Promise<ReflectResult<T>>;

// Update all fixture paths to use .ts extension
const fixturesPath = path.join(__dirname, "fixtures");
const processorPath = path.join(fixturesPath, "fixture_processor.ts");
const processorFooPath = path.join(fixturesPath, "fixture_processor_foo.ts");
const processorBarPath = path.join(fixturesPath, "fixture_processor_bar.ts");
const processorSlowPath = path.join(fixturesPath, "fixture_processor_slow.ts");
const processorCallbackPath = path.join(
  fixturesPath,
  "fixture_processor_callback.ts"
);
const processorCallbackFailPath = path.join(
  fixturesPath,
  "fixture_processor_callback_fail.ts"
);
const processorCrashPath = path.join(
  fixturesPath,
  "fixture_processor_crash.ts"
);
const processorDataPath = path.join(fixturesPath, "fixture_processor_data.ts");
const processorDiscardPath = path.join(
  fixturesPath,
  "fixture_processor_discard.ts"
);
const processorExitPath = path.join(fixturesPath, "fixture_processor_exit.ts");
const processorProgressPath = path.join(
  fixturesPath,
  "fixture_processor_progress.ts"
);
const processorFailPath = path.join(fixturesPath, "fixture_processor_fail.ts");
const processorBrokenPath = path.join(
  fixturesPath,
  "fixture_processor_broken.ts"
);

describe("sandboxed process", () => {
  let queue: Queue;
  let client: Redis;

  beforeEach(async () => {
    client = new Redis({ filename: ":memory:" });
    await client.flushdb();
    queue = buildQueue("test process", {
      redis: client,
      settings: {
        guardInterval: 300000,
        stalledInterval: 300000,
      },
    });
    return queue;
  });

  afterEach(async () => {
    await queue.close();
    await client.flushall();
    await client.quit();
  });

  it("should process and complete", async () => {
    const processFile = processorPath;
    queue.process(processFile);

    const completedPromise = new Promise<void>((resolve, reject) => {
      queue.on("completed", (job, value) => {
        try {
          expect(job.data).toEqual({ foo: "bar" });
          expect(value).toBe(42);
          expect(Object.keys(queue.childPool.retained)).toHaveLength(0);
          expect(queue.childPool.free[processFile]).toHaveLength(1);
          resolve();
        } catch (err) {
          reject(err);
        }
      });
    });

    await queue.add({ foo: "bar" });
    await completedPromise;
  });

  it("should process with named processor", async () => {
    const processFile = processorPath;
    queue.process("foobar", processFile);

    const completedPromise = new Promise<void>((resolve, reject) => {
      queue.on("completed", (job, value) => {
        try {
          expect(job.data).toEqual({ foo: "bar" });
          expect(value).toBe(42);
          expect(Object.keys(queue.childPool.retained)).toHaveLength(0);
          expect(queue.childPool.free[processFile]).toHaveLength(1);
          resolve();
        } catch (err) {
          reject(err);
        }
      });
    });

    await queue.add("foobar", { foo: "bar" });
    await completedPromise;
  });

  it("should process with several named processors", async () => {
    const processFileFoo = processorFooPath;
    const processFileBar = processorBarPath;

    queue.process("foo", processFileFoo);
    queue.process("bar", processFileBar);

    let count = 0;
    const completedPromise = new Promise<void>((resolve, reject) => {
      queue.on("completed", (job, value) => {
        count++;
        try {
          if (count === 1) {
            expect(job.data).toEqual({ foo: "bar" });
            expect(value).toBe("foo");
            expect(Object.keys(queue.childPool.retained)).toHaveLength(1);
            expect(queue.childPool.free[processFileFoo]).toHaveLength(1);
          } else {
            expect(job.data).toEqual({ bar: "qux" });
            expect(value).toBe("bar");
            expect(Object.keys(queue.childPool.retained)).toHaveLength(0);
            expect(queue.childPool.free[processFileBar]).toHaveLength(1);
            resolve();
          }
        } catch (err) {
          reject(err);
        }
      });
    });

    await queue.add("foo", { foo: "bar" });
    await delay(500);
    await queue.add("bar", { bar: "qux" });
    await completedPromise;
  });

  it("should process with concurrent processors", async () => {
    const processFile = processorSlowPath;
    let completed = 0;

    const completedPromise = new Promise<void>((resolve, reject) => {
      queue.on("completed", (job, value) => {
        try {
          expect(value).toBe(42);
          expect(
            Object.keys(queue.childPool.retained).length +
              queue.childPool.getAllFree().length
          ).toBe(4);
          completed++;
          if (completed === 4) {
            expect(queue.childPool.getAllFree()).toHaveLength(4);
            resolve();
          }
        } catch (err) {
          reject(err);
        }
      });
    });

    await Promise.all([
      queue.add({ foo: "bar1" }),
      queue.add({ foo: "bar2" }),
      queue.add({ foo: "bar3" }),
      queue.add({ foo: "bar4" }),
    ]);

    queue.process(4, processFile);
    await completedPromise;
  });

  it("should reuse process with single processors", async () => {
    const processFile = processorSlowPath;
    let completed = 0;

    const completedPromise = new Promise<void>((resolve, reject) => {
      queue.on("completed", (job, value) => {
        try {
          expect(value).toBe(42);
          expect(
            Object.keys(queue.childPool.retained).length +
              queue.childPool.getAllFree().length
          ).toBe(1);
          completed++;
          if (completed === 4) {
            expect(queue.childPool.getAllFree()).toHaveLength(1);
            resolve();
          }
        } catch (err) {
          reject(err);
        }
      });
    });

    await Promise.all([
      queue.add({ foo: "bar1" }),
      queue.add({ foo: "bar2" }),
      queue.add({ foo: "bar3" }),
      queue.add({ foo: "bar4" }),
    ]);

    queue.process(processFile);
    await completedPromise;
  }, 5000);

  it("should process and complete using done", async () => {
    const processFile = processorCallbackPath;
    queue.process(processFile);

    const completedPromise = new Promise<void>((resolve, reject) => {
      queue.on("completed", (job, value) => {
        try {
          expect(job.data).toEqual({ foo: "bar" });
          expect(value).toBe(42);
          expect(Object.keys(queue.childPool.retained)).toHaveLength(0);
          expect(queue.childPool.getAllFree()).toHaveLength(1);
          resolve();
        } catch (err) {
          reject(err);
        }
      });
    });

    await queue.add({ foo: "bar" });
    await completedPromise;
  });

  it("should process and update progress", async () => {
    const processFile = processorProgressPath;
    queue.process(processFile);

    const progresses: number[] = [];
    queue.on("progress", (job, progress) => {
      progresses.push(progress);
    });

    const completedPromise = new Promise<void>((resolve, reject) => {
      queue.on("completed", async (job, value) => {
        try {
          expect(job.data).toEqual({ foo: "bar" });
          expect(value).toBe(37);
          expect(await job.progress()).toBe(100);
          expect(progresses).toEqual([10, 27, 78, 100]);
          expect(Object.keys(queue.childPool.retained)).toHaveLength(0);
          expect(queue.childPool.getAllFree()).toHaveLength(1);

          const logs = await queue.getJobLogs(job.id);
          expect(logs).toEqual({
            logs: ["10", "27", "78", "100"],
            count: 4,
          });

          const logs2 = await queue.getJobLogs(job.id, 2, 2);
          expect(logs2).toEqual({
            logs: ["78"],
            count: 4,
          });

          const logs3 = await queue.getJobLogs(job.id, 0, 1);
          expect(logs3).toEqual({
            logs: ["10", "27"],
            count: 4,
          });

          const logs4 = await queue.getJobLogs(job.id, 1, 2);
          expect(logs4).toEqual({
            logs: ["27", "78"],
            count: 4,
          });

          const logs5 = await queue.getJobLogs(job.id, 2, 2, false);
          expect(logs5).toEqual({
            logs: ["27"],
            count: 4,
          });

          const logs6 = await queue.getJobLogs(job.id, 0, 1, false);
          expect(logs6).toEqual({
            logs: ["100", "78"],
            count: 4,
          });

          const logs7 = await queue.getJobLogs(job.id, 1, 2, false);
          expect(logs7).toEqual({
            logs: ["78", "27"],
            count: 4,
          });

          resolve();
        } catch (err) {
          reject(err);
        }
      });
    });

    await queue.add({ foo: "bar" });
    await completedPromise;
  });

  it("should process and update data", async () => {
    const processFile = processorDataPath;
    queue.process(processFile);

    const completedPromise = new Promise<void>((resolve, reject) => {
      queue.on("completed", (job, value) => {
        try {
          expect(job.data).toEqual({ baz: "qux" });
          expect(value).toEqual({ baz: "qux" });
          expect(Object.keys(queue.childPool.retained)).toHaveLength(0);
          expect(queue.childPool.getAllFree()).toHaveLength(1);
          resolve();
        } catch (err) {
          reject(err);
        }
      });
    });

    await queue.add({ foo: "bar" });
    await completedPromise;
  });

  it("should process, discard and fail without retry", async () => {
    const processFile = processorDiscardPath;
    queue.process(processFile);

    const failedPromise = new Promise<void>((resolve, reject) => {
      queue.on("failed", (job, err) => {
        try {
          expect(job.data).toEqual({ foo: "bar" });
          expect(job.isDiscarded()).toBe(true);
          expect(job.failedReason).toBe("Manually discarded processor");
          expect(err.message).toBe("Manually discarded processor");
          expect(err.stack).toContain("fixture_processor_discard");
          expect(Object.keys(queue.childPool.retained)).toHaveLength(0);
          expect(queue.childPool.getAllFree()).toHaveLength(1);
          resolve();
        } catch (err) {
          reject(err);
        }
      });
    });

    await queue.add({ foo: "bar" });
    await failedPromise;
  });

  it("should process and fail", async () => {
    const processFile = processorFailPath;
    queue.process(processFile);

    const failedPromise = new Promise<void>((resolve, reject) => {
      queue.on("failed", (job, err) => {
        try {
          expect(job.data).toEqual({ foo: "bar" });
          expect(job.failedReason).toBe("Manually failed processor");
          expect(err.message).toBe("Manually failed processor");
          expect(err.stack).toContain("fixture_processor_fail.ts");
          expect(Object.keys(queue.childPool.retained)).toHaveLength(0);
          expect(queue.childPool.getAllFree()).toHaveLength(1);
          resolve();
        } catch (err) {
          reject(err);
        }
      });
    });

    await queue.add({ foo: "bar" });
    await failedPromise;
  });

  it("should error if processor file is missing", () => {
    expect(() => {
      queue.process(path.join(__dirname, "/fixtures/missing_processor.ts"));
    }).toThrow();
  });

  it("should process and fail using callback", async () => {
    const processFile = processorCallbackFailPath;
    queue.process(processFile);

    const failedPromise = new Promise<void>((resolve, reject) => {
      queue.on("failed", (job, err) => {
        try {
          expect(job.data).toEqual({ foo: "bar" });
          expect(job.failedReason).toBe("Manually failed processor");
          expect(err.message).toBe("Manually failed processor");
          expect(Object.keys(queue.childPool.retained)).toHaveLength(0);
          expect(queue.childPool.getAllFree()).toHaveLength(1);
          resolve();
        } catch (err) {
          reject(err);
        }
      });
    });

    await queue.add({ foo: "bar" });
    await failedPromise;
  });

  it("should fail if the process crashes", async () => {
    queue.process(processorCrashPath);

    const job = await queue.add({});
    const result = await typedReflect(Promise.resolve(job.finished()));

    expect(result.isRejected).toBe(true);
    expect(result.reason?.message).toBe("boom!");
  });

  it("should fail if the process exits 0", async () => {
    queue.process(processorCrashPath);

    const job = await queue.add({ exitCode: 0 });
    const result = await typedReflect(Promise.resolve(job.finished()));

    expect(result.isRejected).toBe(true);
    expect(result.reason?.message).toBe("Unexpected exit code: 0 signal: null");
  });

  it("should fail if the process exits non-0", async () => {
    queue.process(processorCrashPath);

    const job = await queue.add({ exitCode: 1 });
    const result = await typedReflect(Promise.resolve(job.finished()));

    expect(result.isRejected).toBe(true);
    expect(result.reason?.message).toBe("Unexpected exit code: 1 signal: null");
  });

  it("should remove exited process", async () => {
    const processFile = processorExitPath;
    queue.process(processFile);

    const completedPromise = new Promise<void>((resolve, reject) => {
      queue.on("completed", async () => {
        try {
          expect(Object.keys(queue.childPool.retained)).toHaveLength(0);
          expect(queue.childPool.getAllFree()).toHaveLength(1);
          await delay(500);
          expect(Object.keys(queue.childPool.retained)).toHaveLength(0);
          expect(queue.childPool.getAllFree()).toHaveLength(0);
          resolve();
        } catch (err) {
          reject(err);
        }
      });
    });

    await queue.add({ foo: "bar" });
    await completedPromise;
  });

  it("should allow the job to complete and then exit on clean", async () => {
    const processFile = processorSlowPath;
    queue.process(processFile);

    // acquire and release a child here so we know it has its full termination handler setup
    const expectedChild = await queue.childPool.retain(processFile);
    queue.childPool.release(expectedChild);

    const onActive = new Promise((resolve) => queue.once("active", resolve));
    const jobAddPromise = queue.add({ foo: "bar" });

    await onActive;

    // at this point the job should be active and running on the child
    expect(Object.keys(queue.childPool.retained)).toHaveLength(1);
    expect(queue.childPool.getAllFree()).toHaveLength(0);
    const child = Object.values(queue.childPool.retained)[0];
    expect(child).toBe(expectedChild);
    expect(child.exitCode).toBeNull();
    expect(child.finished).toBeUndefined();

    // trigger a clean while we know it's doing work
    await queue.childPool.clean();

    // ensure the child did get cleaned up
    expect(expectedChild.killed).toBe(true);
    expect(Object.keys(queue.childPool.retained)).toHaveLength(0);
    expect(queue.childPool.getAllFree()).toHaveLength(0);

    // make sure the job completed successfully
    const job = await jobAddPromise;
    const jobResult = await job.finished();
    expect(jobResult).toBe(42);
  });

  it("should share child pool across all different queues created", async () => {
    const [queueA, queueB] = await Promise.all([
      buildQueue("queueA", { settings: { isSharedChildPool: true } }),
      buildQueue("queueB", { settings: { isSharedChildPool: true } }),
    ]);

    const processFile = processorPath;
    queueA.process(processFile);
    queueB.process(processFile);

    await Promise.all([queueA.add({}), queueB.add({})]);

    expect(queueA.childPool).toBe(queueB.childPool);
  });

  it("should not share childPool across different queues if isSharedChildPool isn't specified", async () => {
    const [queueA, queueB] = await Promise.all([
      buildQueue("queueA", { settings: { isSharedChildPool: false } }),
      buildQueue("queueB"),
    ]);

    const processFile = processorPath;
    queueA.process(processFile);
    queueB.process(processFile);

    await Promise.all([queueA.add({}), queueB.add({})]);

    expect(queueA.childPool).not.toBe(queueB.childPool);
  });

  it("should fail if the process file is broken", async () => {
    const processFile = processorBrokenPath;
    queue.process(processFile);

    const failedPromise = new Promise<void>((resolve) => {
      queue.on("failed", () => {
        resolve();
      });
    });

    await queue.add("test", {});
    await failedPromise;
  });
});
