import { Redis } from "../../ioredis-sqlite";
import { buildQueue, Queue } from "./utils";
import Bull from "bull";

// Helper function to check if Redis is ready
const isRedisReady = async (client: any): Promise<void> => {
  if (!client) {
    throw new Error("Client is not defined");
  }
  return new Promise((resolve, reject) => {
    if (client.status === "ready") {
      resolve();
    } else {
      client.once("ready", resolve);
      client.once("error", reject);
    }
  });
};

describe("connection", () => {
  let client: Redis;

  beforeEach(async () => {
    client = new Redis({ filename: ":memory:" });
    await new Promise<void>((resolve) => client.on("ready", resolve));
    await client.flushdb();
  });

  afterEach(async () => {
    await client.quit();
  });

  it("should fail if reusing connections with invalid options", () => {
    const errMsg = "Redis connection options must be provided";

    const client = new Redis();

    const opts = {
      createClient(type: string) {
        switch (type) {
          case "client":
            return client;
          default:
            return new Redis();
        }
      },
    };
    const queue = buildQueue("external connections", opts);
    expect(queue).toBeDefined();

    try {
      // eslint-disable-next-line no-unused-vars
      const _ = (queue as any).bclient;
      throw new Error("should fail with invalid redis options");
    } catch (err: any) {
      expect(err.message).toBe(errMsg);
    }

    try {
      // eslint-disable-next-line no-unused-vars
      const _ = queue.eclient;
      throw new Error("should fail with invalid redis options");
    } catch (err: any) {
      expect(err.message).toBe(errMsg);
    }
  });

  it("should recover from a connection loss", async () => {
    const queue = buildQueue();
    queue.on("error", () => {
      // error event has to be observed or the exception will bubble up
    });

    const done = new Promise<void>((resolve, reject) => {
      queue.process("processor");
      queue.on("completed", () => {
        queue.close();
        resolve();
      });
    });

    // Simulate disconnect
    await queue.isReady();
    await isRedisReady(queue.client);
    (queue.client as any).stream.end();
    queue.client.emit("error", new Error("ECONNRESET"));

    // add something to the queue
    await queue.add("test", { foo: "bar" });

    await done;
  });

  it("should handle jobs added before and after a redis disconnect", async () => {
    let count = 0;
    const queue = buildQueue();

    await new Promise<void>((resolve, reject) => {
      queue.process("processor");
      queue.on("completed", (job: any) => {
        if (count === 0) {
          expect(job.data.foo).toBe("bar");
        } else {
          queue.close().then(resolve).catch(reject);
        }
        count++;
      });

      queue.on("completed", () => {
        if (count === 1) {
          (queue.client as any).stream.end();
          queue.client.emit("error", new Error("ECONNRESET"));
        }
      });

      queue.isReady().then(() => {
        queue.add("test", { foo: "bar" });
      });

      queue.on("error", () => {
        if (count === 1) {
          queue.add("test", { foo: "bar" });
        }
      });
    });
  });

  it("should not close external connections", async () => {
    const redisOpts = {
      maxRetriesPerRequest: undefined,
      enableReadyCheck: false,
    };

    const client = new Redis(redisOpts);
    const subscriber = new Redis(redisOpts);

    const opts = {
      createClient(type: string) {
        switch (type) {
          case "client":
            return client;
          case "subscriber":
            return subscriber;
          default:
            return new Redis();
        }
      },
    };

    const testQueue = buildQueue("external connections", opts);

    await new Promise<void>((resolve) => {
      if (subscriber.status === "ready") {
        return resolve();
      }
      subscriber.once("ready", resolve);
    });

    await testQueue.isReady();
    await testQueue.add("test", { foo: "bar" });

    expect(testQueue.client).toBe(client);
    expect(testQueue.eclient).toBe(subscriber);

    await testQueue.close();

    expect(client.status).toBe("ready");
    expect(subscriber.status).toBe("ready");
    await Promise.all([client.quit(), subscriber.quit()]);
  });

  it("should fail if redis connection fails and does not reconnect", async () => {
    const queue = buildQueue("connection fail 123", {
      redis: {
        host: "localhost",
        port: 1234,
        retryStrategy: () => null,
      },
    });
    try {
      await isRedisReady(queue.client);
      throw new Error("Did not fail connecting to invalid redis instance");
    } catch (err: any) {
      expect(err.code).toBe("ECONNREFUSED");
      await queue.close();
    }
  });

  it("should close cleanly if redis connection fails", async () => {
    const queue = new Bull("connection fail", {
      redis: {
        host: "localhost",
        port: 1235,
        retryStrategy: () => null,
      },
    });

    await queue.close();
  });

  it("should accept ioredis options on the query string", async () => {
    const queue = new Bull("connection query string", {
      redis: "redis://localhost?tls=RedisCloudFixed",
    });

    expect((queue.client as any).options).toHaveProperty("tls");
    expect((queue.client as any).options.tls).toHaveProperty("ca");

    await queue.close();
  });
});
