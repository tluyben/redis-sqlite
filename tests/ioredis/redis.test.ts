import { Redis } from "../../ioredis-sqlite";

describe("Redis", () => {
  let redis: Redis;

  beforeEach(async () => {
    redis = new Redis({ filename: ":memory:" });
    // Wait for ready event with increased timeout
    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error("Connection timeout"));
      }, 50);

      redis.on("ready", () => {
        clearTimeout(timeout);
        resolve();
      });

      redis.on("error", (err) => {
        clearTimeout(timeout);
        reject(err);
      });
    });
  }, 100);

  afterEach(async () => {
    await redis.quit();
  });

  describe("Connection", () => {
    it("should emit ready event on connection", async () => {
      const redis = new Redis();
      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error("Connection timeout"));
        }, 50);

        redis.on("ready", async () => {
          clearTimeout(timeout);
          expect(redis.status).toBe("ready");
          expect(redis.isReady).toBe(true);
          expect(redis.connected).toBe(true);
          await redis.quit();
          resolve();
        });

        redis.on("error", (err) => {
          clearTimeout(timeout);
          reject(err);
        });
      });
    });

    it("should connect with password if provided", async () => {
      const redis = new Redis({ password: "secret", filename: ":memory:" });
      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error("Connection timeout"));
        }, 50);

        redis.on("ready", async () => {
          clearTimeout(timeout);
          expect(redis.status).toBe("ready");
          expect(redis.isReady).toBe(true);
          expect(redis.connected).toBe(true);
          await redis.quit();
          resolve();
        });

        redis.on("error", (err) => {
          clearTimeout(timeout);
          reject(err);
        });
      });
    });
  });

  describe("String operations", () => {
    it("should set and get string values", async () => {
      await redis.set("key", "value");
      const result = await redis.get("key");
      expect(result).toBe("value");
    });

    it("should handle non-existent keys", async () => {
      const result = await redis.get("nonexistent");
      expect(result).toBeNull();
    });

    it("should set with expiry", async () => {
      await redis.set("key", "value", "EX", 1);
      let result = await redis.get("key");
      expect(result).toBe("value");

      // Wait for expiration
      await new Promise((resolve) => setTimeout(resolve, 1100));

      result = await redis.get("key");
      expect(result).toBeNull();
    });
  });

  describe("Key operations", () => {
    it("should check if key exists", async () => {
      await redis.set("key", "value");
      const exists = await redis.exists("key");
      expect(exists).toBe(1);
    });

    it("should delete keys", async () => {
      await redis.set("key", "value");
      const deleted = await redis.del("key");
      expect(deleted).toBe(1);
      const exists = await redis.exists("key");
      expect(exists).toBe(0);
    });
  });

  describe("Pipeline", () => {
    it("should execute commands in pipeline", async () => {
      const pipeline = redis.multi();
      pipeline.set("key1", "value1");
      pipeline.set("key2", "value2");
      pipeline.get("key1");
      pipeline.get("key2");

      const results = await pipeline.exec();
      expect(results).toHaveLength(4);
      expect(results[0][1]).toBe("OK");
      expect(results[1][1]).toBe("OK");
      expect(results[2][1]).toBe("value1");
      expect(results[3][1]).toBe("value2");
    });
  });

  describe("Hash operations", () => {
    it("should set and get hash fields", async () => {
      await redis.hset("hash", "field", "value");
      const result = await redis.hget("hash", "field");
      expect(result).toBe("value");
    });

    it("should set multiple hash fields", async () => {
      await redis.hmset("hash", { field1: "value1", field2: "value2" });
      const results = await redis.hmget("hash", "field1", "field2");
      expect(results).toEqual(["value1", "value2"]);
    });
  });

  describe("List operations", () => {
    it("should push and pop values", async () => {
      await redis.lpush("list", "value1", "value2");
      const result = await redis.lpop("list");
      expect(result).toBe("value2");
    });

    it("should handle rpoplpush", async () => {
      await redis.rpush("source", "value1", "value2");
      const result = await redis.rpoplpush("source", "destination");
      expect(result).toBe("value2");
      const destinationValue = await redis.lpop("destination");
      expect(destinationValue).toBe("value2");
    });
  });

  describe("Set operations", () => {
    it("should add and check members", async () => {
      await redis.sadd("set", "member1", "member2");
      const isMember = await redis.sismember("set", "member1");
      expect(isMember).toBe(1);
      const members = await redis.smembers("set");
      expect(members.sort()).toEqual(["member1", "member2"].sort());
    });
  });
});
