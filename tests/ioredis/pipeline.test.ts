import { Redis } from "../../ioredis-sqlite";

describe("Pipeline", () => {
  let redis: Redis;

  beforeEach(async () => {
    redis = new Redis({ filename: ":memory:" });
    await new Promise<void>((resolve) => redis.on("ready", resolve));
  });

  afterEach(async () => {
    await redis.quit();
  });

  describe("Pipeline", () => {
    it("should handle multiple operations in sequence", async () => {
      const pipeline = redis.multi();
      pipeline.set("foo", "bar");
      pipeline.get("foo");
      pipeline.set("foo", "baz");
      pipeline.get("foo");

      const results = await pipeline.exec();
      expect(results).toHaveLength(4);
      expect(results[0][1]).toBe("OK");
      expect(results[1][1]).toBe("bar");
      expect(results[2][1]).toBe("OK");
      expect(results[3][1]).toBe("baz");
    });

    it("should handle errors in pipeline", async () => {
      const pipeline = redis.multi();
      pipeline.set("foo", "bar");
      pipeline.hget("foo", "field"); // This should fail as foo is a string
      pipeline.get("foo");

      const results = await pipeline.exec();
      expect(results).toHaveLength(3);
      expect(results[0][1]).toBe("OK");
      expect(results[1][0]).toBeInstanceOf(Error);
      expect(results[2][1]).toBe("bar");
    });

    it("should handle empty pipeline", async () => {
      const pipeline = redis.multi();
      const results = await pipeline.exec();
      expect(results).toHaveLength(0);
    });

    it("should handle multiple key operations", async () => {
      const pipeline = redis.multi();
      pipeline.set("key1", "value1");
      pipeline.set("key2", "value2");
      pipeline.mget("key1", "key2");

      const results = await pipeline.exec();
      expect(results).toHaveLength(3);
      expect(results[0][1]).toBe("OK");
      expect(results[1][1]).toBe("OK");
      expect(results[2][1]).toEqual(["value1", "value2"]);
    });

    it("should handle hash operations in pipeline", async () => {
      const pipeline = redis.multi();
      pipeline.hset("hash", "field1", "value1");
      pipeline.hset("hash", "field2", "value2");
      pipeline.hget("hash", "field1");
      pipeline.hget("hash", "field2");

      const results = await pipeline.exec();
      expect(results).toHaveLength(4);
      expect(results[0][1]).toBe(1);
      expect(results[1][1]).toBe(1);
      expect(results[2][1]).toBe("value1");
      expect(results[3][1]).toBe("value2");
    });

    it("should handle list operations in pipeline", async () => {
      const pipeline = redis.multi();
      pipeline.lpush("list", "value1");
      pipeline.lpush("list", "value2");
      pipeline.lrange("list", 0, -1);

      const results = await pipeline.exec();
      expect(results).toHaveLength(3);
      expect(results[0][1]).toBe(1);
      expect(results[1][1]).toBe(2);
      expect(results[2][1]).toEqual(["value2", "value1"]);
    });

    it("should handle set operations in pipeline", async () => {
      const pipeline = redis.multi();
      pipeline.sadd("set", "member1");
      pipeline.sadd("set", "member2");
      pipeline.smembers("set");

      const results = await pipeline.exec();
      expect(results).toHaveLength(3);
      expect(results[0][1]).toBe(1);
      expect(results[1][1]).toBe(1);
      expect(new Set(results[2][1])).toEqual(new Set(["member1", "member2"]));
    });
  });
});
