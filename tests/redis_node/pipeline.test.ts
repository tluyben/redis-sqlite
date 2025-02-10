import { createClient, RedisClient } from "../../node-redis-sqlite";

describe("Pipeline", () => {
  let redis: RedisClient;

  beforeEach(async () => {
    redis = await createClient({ filename: ":memory:" });
  });

  afterEach(async () => {
    await redis.quit();
  });

  describe("Pipeline", () => {
    it("should handle multiple operations in sequence", async () => {
      const results = await redis
        .multi()
        .set("foo", "bar")
        .get("foo")
        .set("foo", "baz")
        .get("foo")
        .exec();

      expect(results).toHaveLength(4);
      expect(results[0]).toBe("OK");
      expect(results[1]).toBe("bar");
      expect(results[2]).toBe("OK");
      expect(results[3]).toBe("baz");
    });

    it("should handle errors in pipeline", async () => {
      await redis.set("foo", "bar");
      const pipeline = redis.multi()
        .set("foo", "bar")
        .hGet("foo", "field") // This should fail as foo is a string
        .get("foo");

      await expect(pipeline.exec()).rejects.toThrow(/wrong kind of value/);
    });

    it("should handle empty pipeline", async () => {
      const results = await redis.multi().exec();
      expect(results).toHaveLength(0);
    });

    it("should handle multiple key operations", async () => {
      const results = await redis
        .multi()
        .set("key1", "value1")
        .set("key2", "value2")
        .get("key1")
        .get("key2")
        .exec();

      expect(results).toHaveLength(4);
      expect(results[0]).toBe("OK");
      expect(results[1]).toBe("OK");
      expect(results[2]).toBe("value1");
      expect(results[3]).toBe("value2");
    });

    it("should handle hash operations in pipeline", async () => {
      const results = await redis
        .multi()
        .hSet("hash", "field1", "value1")
        .hSet("hash", "field2", "value2")
        .hGet("hash", "field1")
        .hGet("hash", "field2")
        .exec();

      expect(results).toHaveLength(4);
      expect(results[0]).toBe(1);
      expect(results[1]).toBe(1);
      expect(results[2]).toBe("value1");
      expect(results[3]).toBe("value2");
    });

    it("should handle list operations in pipeline", async () => {
      const results = await redis
        .multi()
        .lPush("list", "value1")
        .lPush("list", "value2")
        .lPop("list")
        .lPop("list")
        .exec();

      expect(results).toHaveLength(4);
      expect(results[0]).toBe(1);
      expect(results[1]).toBe(2);
      expect(results[2]).toBe("value2");
      expect(results[3]).toBe("value1");
    });

    it("should handle set operations in pipeline", async () => {
      const results = await redis
        .multi()
        .sAdd("set", "member1")
        .sAdd("set", "member2")
        .sRem("set", "member1")
        .sAdd("set", "member3")
        .exec();

      expect(results).toHaveLength(4);
      expect(results[0]).toBe(1); // First add
      expect(results[1]).toBe(1); // Second add
      expect(results[2]).toBe(1); // Remove
      expect(results[3]).toBe(1); // Third add
    });
  });
});
