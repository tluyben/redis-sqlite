import { createClient, RedisClient } from "../../node-redis-sqlite";

describe("Transaction", () => {
  let redis: RedisClient;

  beforeEach(async () => {
    redis = await createClient({ filename: ":memory:" });
  });

  afterEach(async () => {
    await redis.quit();
  });

  describe("multi/exec", () => {
    it("should work like pipeline by default", async () => {
      const results = await redis
        .multi()
        .set("foo", "transaction")
        .get("foo")
        .exec();

      expect(results).toEqual([
        "OK",
        "transaction"
      ]);
    });

    it("should handle runtime errors", async () => {
      await redis.set("foo", "bar");
      const transaction = redis
        .multi()
        .set("foo", "bar")
        .lPush("foo", "abc") // Should fail since foo is a string
        .get("foo");

      try {
        const results = await transaction.exec();
        // If we get here, the transaction didn't throw as expected
        throw new Error(`Expected transaction to throw but got results: ${JSON.stringify(results)}`);
      } catch (error) {
        expect(error instanceof Error).toBe(true);
        expect((error as Error).message).toMatch(/wrong kind of value/);
        // Verify the state after error
        const value = await redis.get("foo");
        expect(value).toBe("bar"); // First command should have executed
      }
    });

    it("should handle multiple operations in transaction", async () => {
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

    it("should handle hash operations in transaction", async () => {
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

    it("should handle list operations in transaction", async () => {
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

    it("should handle set operations in transaction", async () => {
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

    it("should handle operations on wrong types", async () => {
      await redis.set("string", "value");
      const results = await redis
        .multi()
        .set("string", "new-value")
        .hSet("string", "field", "value") // This will execute but not affect the string
        .get("string")
        .exec();

      // Verify the string was updated but no hash field was set
      expect(results).toHaveLength(3);
      expect(results[0]).toBe("OK"); // set succeeds
      expect(results[2]).toBe("new-value"); // get shows the updated string value
      
      // Verify no hash field was actually set
      await expect(redis.hGet("string", "field")).rejects.toThrow(/wrong kind of value/);
    });

    it("should handle empty transaction", async () => {
      const results = await redis.multi().exec();
      expect(results).toHaveLength(0);
    });
  });
});
