import { Redis } from "../../ioredis-sqlite";

describe("Transaction", () => {
  let redis: Redis;

  beforeEach(async () => {
    redis = new Redis({ filename: ":memory:" });
    await new Promise<void>((resolve) => redis.on("ready", resolve));
  });

  afterEach(async () => {
    await redis.quit();
  });

  describe("multi/exec", () => {
    it("should work like pipeline by default", async () => {
      const result = await redis
        .multi()
        .set("foo", "transaction")
        .get("foo")
        .exec();

      expect(result).toEqual([
        [null, "OK"],
        [null, "transaction"]
      ]);
    });

    it("should handle runtime errors correctly", async () => {
      const result = await redis
        .multi()
        .set("foo", "bar")
        .lpush("foo", "abc") // Should fail since foo is a string
        .exec();

      expect(result).toHaveLength(2);
      expect(result[0]).toEqual([null, "OK"]);
      expect(result[1][0]).toBeInstanceOf(Error);
      expect(result[1][0]!.toString()).toMatch(/wrong kind of value/);
    });

    it("should handle multiple operations in transaction", async () => {
      const result = await redis
        .multi()
        .set("key1", "value1")
        .set("key2", "value2")
        .get("key1")
        .get("key2")
        .exec();

      expect(result).toHaveLength(4);
      expect(result[0]).toEqual([null, "OK"]);
      expect(result[1]).toEqual([null, "OK"]);
      expect(result[2]).toEqual([null, "value1"]);
      expect(result[3]).toEqual([null, "value2"]);
    });

    it("should handle hash operations in transaction", async () => {
      const result = await redis
        .multi()
        .hset("hash", "field1", "value1")
        .hset("hash", "field2", "value2")
        .hget("hash", "field1")
        .hget("hash", "field2")
        .exec();

      expect(result).toHaveLength(4);
      expect(result[0]).toEqual([null, 1]);
      expect(result[1]).toEqual([null, 1]);
      expect(result[2]).toEqual([null, "value1"]);
      expect(result[3]).toEqual([null, "value2"]);
    });

    it("should handle list operations in transaction", async () => {
      const result = await redis
        .multi()
        .lpush("list", "value1")
        .lpush("list", "value2")
        .lrange("list", 0, -1)
        .exec();

      expect(result).toHaveLength(3);
      expect(result[0]).toEqual([null, 1]);
      expect(result[1]).toEqual([null, 2]);
      expect(result[2]).toEqual([null, ["value2", "value1"]]);
    });

    it("should handle set operations in transaction", async () => {
      const result = await redis
        .multi()
        .sadd("set", "member1")
        .sadd("set", "member2")
        .smembers("set")
        .exec();

      expect(result).toHaveLength(3);
      expect(result[0]).toEqual([null, 1]);
      expect(result[1]).toEqual([null, 1]);
      expect(new Set(result[2][1])).toEqual(new Set(["member1", "member2"]));
    });

    it("should handle type errors in transaction", async () => {
      await redis.set("string", "value");
      const result = await redis
        .multi()
        .set("string", "new-value")
        .hset("string", "field", "value") // Should fail since string is a string
        .get("string")
        .exec();

      expect(result).toHaveLength(3);
      expect(result[0]).toEqual([null, "OK"]);
      expect(result[1][0]).toBeInstanceOf(Error);
      expect(result[1][0]!.toString()).toMatch(/wrong kind of value/);
      expect(result[2]).toEqual([null, "new-value"]);
    });

    it("should handle empty transaction", async () => {
      const result = await redis.multi().exec();
      expect(result).toHaveLength(0);
    });
  });
});
