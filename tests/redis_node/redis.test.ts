import { createClient, RedisClient } from "../../node-redis-sqlite";

describe("Redis Node", () => {
  let redis: RedisClient;

  beforeEach(async () => {
    redis = await createClient({ filename: ":memory:" });
  });

  afterEach(async () => {
    await redis.quit();
  });

  describe("Connection", () => {
    it("should connect successfully", async () => {
      const client = await createClient({ filename: ":memory:" });
      expect(client.isOpen).toBe(true);
      await client.quit();
    });

    it("should connect with password if provided", async () => {
      const client = await createClient({ 
        filename: ":memory:",
        password: "secret"
      });
      expect(client.isOpen).toBe(true);
      await client.quit();
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
      await redis.set("key", "value");
      await redis.expire("key", 1);
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

  describe("Hash operations", () => {
    it("should set and get hash fields", async () => {
      await redis.hSet("hash", "field", "value");
      const result = await redis.hGet("hash", "field");
      expect(result).toBe("value");
    });

    it("should set multiple hash fields", async () => {
      await redis.hSet("hash", { field1: "value1", field2: "value2" });
      const result1 = await redis.hGet("hash", "field1");
      const result2 = await redis.hGet("hash", "field2");
      expect([result1, result2]).toEqual(["value1", "value2"]);
    });
  });

  describe("List operations", () => {
    it("should push and pop values", async () => {
      await redis.lPush("list", "value1", "value2");
      const result = await redis.lPop("list");
      expect(result).toBe("value2");
    });

    it("should handle rightPopLeftPush", async () => {
      await redis.rPush("source", "value1", "value2");
      const result = await redis.rPopLPush("source", "destination");
      expect(result).toBe("value2");
      const destinationValue = await redis.lPop("destination");
      expect(destinationValue).toBe("value2");
    });
  });

  describe("Set operations", () => {
    it("should add and check members", async () => {
      await redis.sAdd("set", "member1", "member2");
      const isMember = await redis.sIsMember("set", "member1");
      expect(isMember).toBe(1);
      const members = await redis.sMembers("set");
      expect(members.sort()).toEqual(["member1", "member2"].sort());
    });
  });
});
