import { Redis } from "../../ioredis-sqlite";
import Bull, {
  Queue as BullQueue,
  Job as BullJob,
  ProcessCallbackFunction,
} from "bull";
import _ from "lodash";

const STD_QUEUE_NAME = "test queue";

interface QueueChild {
  killed?: boolean;
  finished?: boolean;
  exitCode: number | null;
}

interface ChildPool {
  retained: { [key: string]: any };
  free: { [key: string]: any[] };
  getAllFree(): any[];
  clean(): Promise<void>;
  retain(processFile: string): Promise<any>;
  release(child: any): void;
}

// Our extended job type with additional methods
interface ExtendedJob<T = any> extends Omit<BullJob<T>, "progress"> {
  id: string | number;
  data: T;
  isDiscarded(): boolean;
  progress(): Promise<number>;
  progress(value: number): Promise<void>;
  finished(): Promise<any>;
  failedReason?: string;
}

// Custom Queue type that includes our extensions
type JobStatus =
  | "completed"
  | "failed"
  | "delayed"
  | "active"
  | "waiting"
  | "paused"
  | "wait";

interface QueueSettings {
  stalledInterval: number;
  maxStalledCount: number;
  lockDuration?: number;
  lockRenewTime?: number;
}

interface Queue<T = any>
  extends Omit<
    BullQueue<T>,
    "process" | "add" | "on" | "once" | "addBulk" | "clean"
  > {
  paused?: boolean;
  childPool: ChildPool;
  client: any;
  eclient: any;
  settings: QueueSettings;
  process(processor: string): Promise<void>;
  process(
    name: string,
    processor:
      | string
      | ((
          job: ExtendedJob,
          done?: (error?: Error | null) => void
        ) => void | Promise<any>)
  ): Promise<void>;
  process(concurrency: number, processor: string): Promise<void>;
  getJobLogs(
    jobId: string | number,
    start?: number,
    end?: number,
    asc?: boolean
  ): Promise<{ logs: string[]; count: number }>;
  add(data: T, opts?: any): Promise<ExtendedJob<T>>;
  add(name: string, data: T, opts?: any): Promise<ExtendedJob<T>>;
  addBulk(
    jobs: { name: string; data: any; opts?: any }[]
  ): Promise<ExtendedJob[]>;
  startMoveUnlockedJobsToWait(): void;
  getActive(): Promise<ExtendedJob[]>;
  getWaiting(): Promise<ExtendedJob[]>;
  getCompleted(
    start?: number,
    end?: number,
    opts?: { excludeData?: boolean }
  ): Promise<ExtendedJob[]>;
  getFailed(): Promise<ExtendedJob[]>;
  getJob(jobId: string | number): Promise<ExtendedJob | null>;
  getJobs(
    types: JobStatus[],
    start?: number,
    end?: number,
    asc?: boolean
  ): Promise<ExtendedJob[]>;
  getCountsPerPriority(priorities: number[]): Promise<Record<string, number>>;
  clean(grace: number): Promise<ExtendedJob[]>;
  moveToActive(): Promise<void>;
  toKey(prefix: string): string;
  getMetrics(
    type: "completed" | "failed",
    start?: number,
    end?: number
  ): Promise<{
    meta: { count: number; prevTS: number; prevCount: number };
    data: number[];
    count: number;
  }>;
  obliterate(options?: { force?: boolean }): Promise<void>;
  getRepeatableJobs(): Promise<
    {
      key: string;
      name: string;
      id: string;
      endDate: number;
      tz: string;
      cron: string;
      every: number;
      next: number;
    }[]
  >;
  pause(isLocal?: boolean, doNotWaitActive?: boolean): Promise<void>;
  resume(isLocal?: boolean): Promise<void>;
  isPaused(isLocal?: boolean): Promise<boolean>;
  getJobCounts(): Promise<{
    waiting: number;
    active: number;
    completed: number;
    failed: number;
    delayed: number;
    paused: number;
  }>;
  getJobCountByTypes(types: JobStatus | JobStatus[]): Promise<number>;
  getNextJob(): Promise<ExtendedJob | undefined>;
  empty(): Promise<void>;
  on(event: string, callback: Function): this;
  once(event: string, callback: Function): this;
}

let queues: Queue[] = [];

const originalSetTimeout = setTimeout;

function simulateDisconnect(queue: Queue): void {
  queue.client.disconnect();
  queue.eclient.disconnect();
}

function buildQueue(name?: string, options?: any): Queue {
  options = _.extend(
    {
      redis: new Redis({ filename: ":memory:" }),
    },
    options
  );
  const queue = new Bull(name || STD_QUEUE_NAME, options);
  (queue as any).childPool = {
    retained: {},
    free: {},
    getAllFree: () => [],
    clean: async () => {},
    retain: async () => {},
    release: () => {},
  };
  return queue as unknown as Queue;
}

async function newQueue(name: string, opts?: any): Promise<Queue> {
  const queue = buildQueue(name, opts);
  await queue.isReady();
  return queue;
}

async function cleanupQueue(queue: Queue): Promise<void> {
  await queue.empty();
  await queue.close();
}

async function cleanupQueues(): Promise<void> {
  await Promise.all(
    queues.map((queue) => {
      const errHandler = () => {};
      queue.on("error", errHandler);
      return queue.close().catch(errHandler);
    })
  );
  queues = [];
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    originalSetTimeout(() => {
      resolve();
    }, ms);
  });
}

export {
  simulateDisconnect,
  buildQueue,
  cleanupQueue,
  newQueue,
  cleanupQueues,
  sleep,
  Queue,
  QueueChild,
  ExtendedJob,
  JobStatus,
};
