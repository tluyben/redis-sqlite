import { Redis } from "../../ioredis-sqlite";
import { buildQueue, Queue, ExtendedJob } from "./utils";
import sinon from "sinon";
import moment from "moment";
import _ from "lodash";

const ONE_SECOND = 1000;
const ONE_MINUTE = 60 * ONE_SECOND;
const ONE_HOUR = 60 * ONE_MINUTE;
const ONE_DAY = 24 * ONE_HOUR;
const MAX_INT = 2147483647;

type ProcessFunction = (job: ExtendedJob, done?: (error?: Error | null) => void) => void | Promise<any>;

describe("repeat", () => {
  let queue: Queue;
  let client: Redis;
  let clock: sinon.SinonFakeTimers;

  beforeEach(function () {
    clock = sinon.useFakeTimers();
    client = new Redis();
    return client.flushdb().then(() => {
      queue = buildQueue("repeat", {
        settings: {
          guardInterval: MAX_INT,
          stalledInterval: MAX_INT,
          drainDelay: 1, // Small delay so that .close is faster.
        },
      });
      return queue;
    });
  });

  afterEach(function () {
    clock.restore();
    return queue.close().then(() => {
      return client.quit();
    });
  });

  it("should create multiple jobs if they have the same cron pattern", (done) => {
    const cron = "*/10 * * * * *";
    const customJobIds = ["customjobone", "customjobtwo"];

    Promise.all([
      queue.add({} as any, { jobId: customJobIds[0], repeat: { cron } }),
      queue.add({} as any, { jobId: customJobIds[1], repeat: { cron } }),
    ])
      .then(() => {
        return queue.count();
      })
      .then((count) => {
        expect(count).toBe(2);
        done();
      })
      .catch(done);
  });

  it("should add a repetable job when using stardDate and endDate", async () => {
    const job1 = await queue.add(
      {
        name: "job1",
      } as any,
      {
        repeat: {
          cron: "0 * * * * *",
          startDate: "2020-09-02T22:29:00Z",
        },
      }
    );

    expect(job1).toBeDefined();
    expect(job1.opts).toHaveProperty("repeat");
    expect(job1.opts?.repeat).toEqual({
      count: 1,
      cron: "0 * * * * *",
      startDate: "2020-09-02T22:29:00Z",
      key: "__default__::::0 * * * * *",
    });

    const job2 = await queue.add(
      {
        name: "job2",
      } as any,
      {
        repeat: {
          cron: "0 * * * * *",
          startDate: "2020-09-02T22:29:00Z",
          endDate: "2020-09-05T01:44:37Z",
        },
      }
    );
    expect(job2).toBeDefined();
    expect(job2.opts).toHaveProperty("repeat");
    expect(job2.opts?.repeat).toEqual({
      count: 1,
      cron: "0 * * * * *",
      startDate: "2020-09-02T22:29:00Z",
      endDate: "2020-09-05T01:44:37Z",
      key: "__default__::1599270277000::0 * * * * *",
    });
  });

  it("should get repeatable jobs with different cron pattern", (done) => {
    const crons = [
      "10 * * * * *",
      "2 10 * * * *",
      "1 * * 5 * *",
      "2 * * 4 * *",
    ];

    Promise.all([
      queue.add("first", {} as any, {
        repeat: { cron: crons[0], endDate: 12345 },
      }),
      queue.add("second", {} as any, {
        repeat: { cron: crons[1], endDate: 610000 },
      }),
      queue.add("third", {} as any, {
        repeat: { cron: crons[2], tz: "Africa/Abidjan" },
      }),
      queue.add("fourth", {} as any, {
        repeat: { cron: crons[3], tz: "Africa/Accra" },
      }),
      queue.add("fifth", {} as any, { repeat: { every: 7563 } }),
    ])
      .then(() => {
        return queue.getRepeatableCount();
      })
      .then((count) => {
        expect(count).toBe(5);
        return queue.getRepeatableJobs();
      })
      .then((jobs) => {
        return jobs.sort((a, b) => {
          return crons.indexOf(a.cron) > crons.indexOf(b.cron) ? 1 : -1;
        });
      })
      .then((jobs) => {
        expect(jobs).toHaveLength(5);
        expect(jobs).toContainEqual({
          key: "first::12345::10 * * * * *",
          name: "first",
          id: null,
          endDate: 12345,
          tz: null,
          cron: "10 * * * * *",
          every: null,
          next: 10000,
        });
        expect(jobs).toContainEqual({
          key: "second::610000::2 10 * * * *",
          name: "second",
          id: null,
          endDate: 610000,
          tz: null,
          cron: "2 10 * * * *",
          every: null,
          next: 602000,
        });
        expect(jobs).toContainEqual({
          key: "fourth:::Africa/Accra:2 * * 4 * *",
          name: "fourth",
          id: null,
          endDate: null,
          tz: "Africa/Accra",
          cron: "2 * * 4 * *",
          every: null,
          next: 259202000,
        });
        expect(jobs).toContainEqual({
          key: "third:::Africa/Abidjan:1 * * 5 * *",
          name: "third",
          id: null,
          endDate: null,
          tz: "Africa/Abidjan",
          cron: "1 * * 5 * *",
          every: null,
          next: 345601000,
        });
        expect(jobs).toContainEqual({
          key: "fifth:::7563",
          name: "fifth",
          id: null,
          tz: null,
          endDate: null,
          cron: null,
          every: 7563,
          next: 7563,
        });
        done();
      })
      .catch(done);
  });

  it("should repeat every 2 seconds", function (done) {
    jest.setTimeout(20000);
    const date = new Date("2017-02-07 9:24:00");
    clock.setSystemTime(date);
    const nextTick = 2 * ONE_SECOND + 500;

    queue
      .add("repeat", { foo: "bar" } as any, {
        repeat: { cron: "*/2 * * * * *" },
      })
      .then(() => {
        clock.tick(nextTick);
      });

    const processor: ProcessFunction = (job, done = () => {}) => {
      done();
    };
    queue.process("repeat", processor);

    let prev: ExtendedJob;
    let counter = 0;
    queue.on("completed", (job: ExtendedJob) => {
      clock.tick(nextTick);
      if (prev) {
        expect(prev.timestamp).toBeLessThan(job.timestamp);
        expect(job.timestamp - prev.timestamp).toBeGreaterThanOrEqual(2000);
      }
      prev = job;
      counter++;
      if (counter == 20) {
        done();
      }
    });
  });

  it("should repeat every 2 seconds with startDate in future", function (done) {
    const date = new Date("2017-02-07 9:24:00");
    clock.setSystemTime(date);
    const nextTick = 2 * ONE_SECOND + 500;
    const delay = 5 * ONE_SECOND + 500;

    queue
      .add("repeat", { foo: "bar" } as any, {
        repeat: {
          cron: "*/2 * * * * *",
          startDate: new Date("2017-02-07 9:24:05"),
        },
      })
      .then(() => {
        clock.tick(nextTick + delay);
      });

    const processor: ProcessFunction = (job, done = () => {}) => {
      done();
    };
    queue.process("repeat", processor);

    let prev: ExtendedJob;
    let counter = 0;
    queue.on("completed", (job: ExtendedJob) => {
      clock.tick(nextTick);
      if (prev) {
        expect(prev.timestamp).toBeLessThan(job.timestamp);
        expect(job.timestamp - prev.timestamp).toBeGreaterThanOrEqual(2000);
      }
      prev = job;
      counter++;
      if (counter == 20) {
        done();
      }
    });
  });

  it("should repeat every 2 seconds with startDate in past", function (done) {
    const date = new Date("2017-02-07 9:24:00");
    clock.setSystemTime(date);
    const nextTick = 2 * ONE_SECOND + 500;

    queue
      .add("repeat", { foo: "bar" } as any, {
        repeat: {
          cron: "*/2 * * * * *",
          startDate: new Date("2017-02-07 9:22:00"),
        },
      })
      .then(() => {
        clock.tick(nextTick);
      });

    const processor: ProcessFunction = (job, done = () => {}) => {
      done();
    };
    queue.process("repeat", processor);

    let prev: ExtendedJob;
    let counter = 0;
    queue.on("completed", (job: ExtendedJob) => {
      clock.tick(nextTick);
      if (prev) {
        expect(prev.timestamp).toBeLessThan(job.timestamp);
        expect(job.timestamp - prev.timestamp).toBeGreaterThanOrEqual(2000);
      }
      prev = job;
      counter++;
      if (counter == 20) {
        done();
      }
    });
  });

  it("should repeat once a day for 5 days", function (done) {
    const date = new Date("2017-05-05 13:12:00");
    clock.setSystemTime(date);
    const nextTick = ONE_DAY;

    queue
      .add("repeat", { foo: "bar" } as any, {
        repeat: {
          cron: "0 1 * * *",
          endDate: new Date("2017-05-10 13:12:00"),
        },
      })
      .then(() => {
        clock.tick(nextTick);
      });

    const processor: ProcessFunction = (job, done = () => {}) => {
      done();
    };
    queue.process("repeat", processor);

    let prev: ExtendedJob;
    let counter = 0;
    queue.on("completed", (job: ExtendedJob) => {
      clock.tick(nextTick);
      if (prev) {
        expect(prev.timestamp).toBeLessThan(job.timestamp);
        expect(job.timestamp - prev.timestamp).toBeGreaterThanOrEqual(ONE_DAY);
      }
      prev = job;

      counter++;
      if (counter == 5) {
        queue.getWaiting().then((jobs) => {
          expect(jobs).toHaveLength(0);
          queue.getDelayed().then((jobs) => {
            expect(jobs).toHaveLength(0);
            done();
          });
        });
      }
    });
  });

  it("should repeat 7:th day every month at 9:25", function (done) {
    jest.setTimeout(20000);
    const date = new Date("2017-02-02 7:21:42");
    clock.setSystemTime(date);

    function nextTick() {
      const now = moment();
      const nextMonth = moment().add(1, "months");
      clock.tick(nextMonth.valueOf() - now.valueOf());
    }

    queue
      .add("repeat", { foo: "bar" } as any, {
        repeat: { cron: "* 25 9 7 * *" },
      })
      .then(() => {
        nextTick();
      });

    const processor: ProcessFunction = (job, done = () => {}) => {
      done();
    };
    queue.process("repeat", processor);

    let counter = 20;
    let prev: ExtendedJob;
    queue.on("completed", (job: ExtendedJob) => {
      if (prev) {
        expect(prev.timestamp).toBeLessThan(job.timestamp);
        const diff = moment(job.timestamp).diff(
          moment(prev.timestamp),
          "months",
          true
        );
        expect(diff).toBeGreaterThanOrEqual(1);
      }
      prev = job;

      counter--;
      if (counter == 0) {
        done();
      }
      nextTick();
    });
  });

  it("should create two jobs with the same ids", () => {
    const options = {
      repeat: {
        cron: "0 1 * * *",
      },
    };

    const p1 = queue.add({ foo: "bar" } as any, options);
    const p2 = queue.add({ foo: "bar" } as any, options);

    return Promise.all([p1, p2]).then((jobs) => {
      expect(jobs).toHaveLength(2);
      expect(jobs[0].id).toBe(jobs[1].id);
    });
  });

  it("should allow removing a named repeatable job", function (done) {
    const date = new Date("2017-02-07 9:24:00");
    clock.setSystemTime(date);

    const nextTick = 2 * ONE_SECOND + 250;
    const repeat = { cron: "*/2 * * * * *" };

    queue.add("remove", { foo: "bar" } as any, { repeat }).then(() => {
      clock.tick(nextTick);
    });

    let counter = 0;
    const processor: ProcessFunction = (job, done = () => {}) => {
      counter++;
      if (counter == 20) {
        queue.removeRepeatable("remove", repeat).then(() => {
          clock.tick(nextTick);
          queue.getDelayed().then((delayed) => {
            expect(delayed).toHaveLength(0);
            done();
            done();
          });
        });
      } else if (counter > 20) {
        done(Error("should not repeat more than 20 times"));
      } else {
        done();
      }
    };
    queue.process("remove", processor);

    let prev: ExtendedJob;
    queue.on("completed", (job: ExtendedJob) => {
      clock.tick(nextTick);
      if (prev) {
        expect(prev.timestamp).toBeLessThan(job.timestamp);
        expect(job.timestamp - prev.timestamp).toBeGreaterThanOrEqual(2000);
      }
      prev = job;
    });
  });

  it("should be able to remove repeatable jobs by key", async () => {
    const repeat = { cron: "*/2 * * * * *" };

    await queue.add("remove", { foo: "bar" } as any, { repeat });

    const repeatableJobs = await queue.getRepeatableJobs();
    expect(repeatableJobs).toHaveLength(1);
    await queue.removeRepeatableByKey(repeatableJobs[0].key);

    const repeatableJobs2 = await queue.getRepeatableJobs();
    expect(repeatableJobs2).toHaveLength(0);

    const delayedJobs = await queue.getDelayed();
    expect(delayedJobs).toHaveLength(0);
  });

  it("should return repeatable job key", async () => {
    const repeat = { cron: "*/2 * * * * *" };

    const job = await queue.add("remove", { foo: "bar" } as any, { repeat });

    expect(job.opts?.repeat?.key).toBe("remove::::*/2 * * * * *");
  });

  it("should be able to remove repeatable jobs by key that has a jobId", async () => {
    const repeat = { cron: "*/2 * * * * *" };

    await queue.add("remove", { foo: "bar" } as any, { jobId: "qux", repeat });

    const repeatableJobs = await queue.getRepeatableJobs();
    expect(repeatableJobs).toHaveLength(1);
    await queue.removeRepeatableByKey(repeatableJobs[0].key);

    const repeatableJobs2 = await queue.getRepeatableJobs();
    expect(repeatableJobs2).toHaveLength(0);

    const delayedJobs = await queue.getDelayed();
    expect(delayedJobs).toHaveLength(0);
  });

  it("should allow removing a customId repeatable job", function (done) {
    const date = new Date("2017-02-07 9:24:00");
    clock.setSystemTime(date);

    const nextTick = 2 * ONE_SECOND + 250;
    const repeat = { cron: "*/2 * * * * *" };

    queue
      .add({ foo: "bar" } as any, { repeat: repeat, jobId: "xxxx" })
      .then(() => {
        clock.tick(nextTick);
      });

    let counter = 0;
    const processor: ProcessFunction = (job, done = () => {}) => {
      counter++;
      if (counter == 20) {
        queue
          .removeRepeatable(_.defaults({ jobId: "xxxx" }, repeat))
          .then(() => {
            clock.tick(nextTick);
            queue.getDelayed().then((delayed) => {
              expect(delayed).toHaveLength(0);
              done();
              done();
            });
          });
      } else if (counter > 20) {
        done(Error("should not repeat more than 20 times"));
      } else {
        done();
      }
    };
    queue.process("test", processor);

    let prev: ExtendedJob;
    queue.on("completed", (job: ExtendedJob) => {
      clock.tick(nextTick);
      if (prev) {
        expect(prev.timestamp).toBeLessThan(job.timestamp);
        expect(job.timestamp - prev.timestamp).toBeGreaterThanOrEqual(2000);
      }
      prev = job;
    });
  });

  it("should not re-add a repeatable job after it has been removed", function () {
    const date = new Date("2017-02-07 9:24:00");
    const nextTick = 2 * ONE_SECOND + 250;
    const repeat = { cron: "*/2 * * * * *" };
    const nextRepeatableJob = queue.nextRepeatableJob;
    clock.setSystemTime(date);

    const afterRemoved = new Promise<void>((resolve) => {
      const processor: ProcessFunction = (job, done = () => {}) => {
        queue.nextRepeatableJob = function () {
          const args = arguments;
          // In order to simulate race condition
          // Make removeRepeatables happen any time after a moveToX is called
          return queue
            .removeRepeatable(_.defaults({ jobId: "xxxx" }, repeat))
            .then(() => {
              // nextRepeatableJob will now re-add the removed repeatable
              return nextRepeatableJob.apply(queue, args);
            })
            .then((result) => {
              resolve();
              done();
              return result;
            });
        };
      };
      queue.process("test", processor);

      queue
        .add({ foo: "bar" } as any, { repeat: repeat, jobId: "xxxx" })
        .then(() => {
          clock.tick(nextTick);
        });

      queue.on("completed", () => {
        clock.tick(nextTick);
      });
    });

    return afterRemoved.then(() => {
      return queue.getRepeatableJobs().then((jobs) => {
        // Repeatable job was recreated
        expect(jobs).toHaveLength(0);
      });
    });
  });

  it("should allow adding a repeatable job after removing it", () => {
    const processor: ProcessFunction = (job, done = () => {}) => {
      done();
    };
    queue.process("test", processor);

    const repeat = {
      cron: "*/5 * * * *",
    };

    return queue
      .add(
        "myTestJob",
        {
          data: "2",
        } as any,
        {
          repeat,
        }
      )
      .then(() => {
        return queue.getDelayed();
      })
      .then((delayed) => {
        expect(delayed).toHaveLength(1);
      })
      .then(() => {
        return queue.removeRepeatable("myTestJob", repeat);
      })
      .then(() => {
        return queue.getDelayed();
      })
      .then((delayed) => {
        expect(delayed).toHaveLength(0);
      })
      .then(() => {
        return queue.add(
          "myTestJob",
          {
            data: "2",
          } as any,
          {
            repeat,
          }
        );
      })
      .then(() => {
        return queue.getDelayed();
      })
      .then((delayed) => {
        expect(delayed).toHaveLength(1);
      });
  });

  it("should not repeat more than 5 times", function (done) {
    const date = new Date("2017-02-07 9:24:00");
    clock.setSystemTime(date);
    const nextTick = ONE_SECOND + 500;

    queue
      .add("repeat", { foo: "bar" } as any, {
        repeat: { limit: 5, cron: "*/1 * * * * *" },
      })
      .then(() => {
        clock.tick(nextTick);
      });

    const processor: ProcessFunction = (job, done = () => {}) => {
      done();
    };
    queue.process("repeat", processor);

    let counter = 0;
    queue.on("completed", () => {
      clock.tick(nextTick);
      counter++;
      if (counter == 5) {
        setTimeout(() => {
          done();
        }, nextTick * 2);
      } else if (counter > 5) {
        done(Error("should not repeat more than 5 times"));
      }
    });
  });

  it("should processes delayed jobs by priority", function (done) {
    const date = new Date("2017-02-07 9:24:00");
    clock.setSystemTime(date);
    const nextTick = 1000;

    let currentPriority = 1;
    const jobAdds: Promise<ExtendedJob>[] = [
      queue.add({ p: 1 } as any, { priority: 1, delay: nextTick * 2 }),
      queue.add({ p: 3 } as any, { priority: 3, delay: nextTick * 2 }),
      queue.add({ p: 2 } as any, { priority: 2, delay: nextTick * 2 })
    ];

    Promise.all(jobAdds).then(() => {
      clock.tick(nextTick * 3);

      const processor: ProcessFunction = (job, done = () => {}) => {
        try {
          expect(job.id).toBeDefined();
          expect(job.data.p).toBe(currentPriority++);
          if (currentPriority > 3) {
            done();
            done();
          } else {
            done();
          }
        } catch (err) {
          done(err);
        }
      };
      queue.process("test", processor);
    }, done);
  });

  it('should use ".every" as a valid interval', function (done) {
    const interval = ONE_SECOND * 2;
    const date = new Date("2017-02-07 9:24:00");

    // Quantize time
    const time = Math.floor(date.getTime() / interval) * interval;
    clock.setSystemTime(time);

    const nextTick = ONE_SECOND * 2 + 500;

    queue
      .add("repeat m", { type: "m" } as any, { repeat: { every: interval } })
      .then(() => {
        return queue.add("repeat s", { type: "s" } as any, {
          repeat: { every: interval },
        });
      })
      .then(() => {
        clock.tick(nextTick);
      });

    const processor1: ProcessFunction = (job, done = () => {}) => {
      done();
    };
    queue.process("repeat m", processor1);

    const processor2: ProcessFunction = (job, done = () => {}) => {
      done();
    };
    queue.process("repeat s", processor2);

    let prevType: string;
    let counter = 0;
    queue.on("completed", (job: ExtendedJob) => {
      clock.tick(nextTick);
      if (prevType) {
        expect(prevType).not.toBe(job.data.type);
      }
      prevType = job.data.type;
      counter++;
      if (counter == 20) {
        done();
      }
    });
  });

  it("should throw an error when using .cron and .every simutaneously", (done) => {
    queue
      .add("repeat", { type: "m" } as any, {
        repeat: { every: 5000, cron: "*/1 * * * * *" },
      })
      .then(
        () => {
          throw new Error("The error was not thrown");
        },
        (err) => {
          expect(err.message).toBe(
            "Both .cron and .every options are defined for this repeatable job"
          );
          done();
        }
      );
  });

  it("should emit a waiting event when adding a repeatable job to the waiting list", function (done) {
    const date = new Date("2017-02-07 9:24:00");
    clock.setSystemTime(date);
    const nextTick = 2 * ONE_SECOND + 500;

    queue.on("waiting", (jobId) => {
      expect(jobId).toBe(
        "repeat:93168b0ea97b55fb5a8325e8c66e4300:" +
          (date.getTime() + 2 * ONE_SECOND)
      );
      done();
    });

    queue
      .add("repeat", { foo: "bar" } as any, {
        repeat: { cron: "*/2 * * * * *" },
      })
      .then(() => {
        clock.tick(nextTick);
      });

    const processor: ProcessFunction = (job, done = () => {}) => {
      done();
    };
    queue.process("repeat", processor);
  });

  it("should have the right count value", function (done) {
    queue.add({ foo: "bar" } as any, { repeat: { every: 1000 } }).then(() => {
      clock.tick(ONE_SECOND + 10);
    });

    const processor: ProcessFunction = (job: ExtendedJob, done = () => {}) => {
      if (job.opts?.repeat?.count === 1) {
        done();
        done();
      } else {
        done(Error("repeatable job got the wrong repeat count"));
      }
    };
    queue.process("test", processor);
  });

  it("it should stop repeating after endDate", async function () {
    const every = 100;
    const date = new Date("2017-02-07 9:24:00");
    clock.setSystemTime(date);

    await queue.add({ id: "my id" } as any, {
      repeat: {
        endDate: Date.now() + 1000,
        every: 100,
      },
    });

    clock.tick(every + 1);

    let processed = 0;
    const processor: ProcessFunction = async (job, done = () => {}) => {
      clock.tick(every);
      processed++;
      done();
    };
    queue.process("test", processor);

    await new Promise((resolve) => setTimeout(resolve, 1100));

    const delayed = await queue.getDelayed();

    expect(delayed).toHaveLength(0);
    expect(processed).toBe(10);
  });
});
