import { DB, Cmp } from "typepersist";
import { EventEmitter } from "events";

const TABLE_PREFIX = process.env.REDIS_SQLITE_PREFIX || "redis_";

interface RedisOptions {
  filename?: string; // :memory: or file path
  password?: string; // Redis auth password
}

interface Record {
  id: number;
  key: string;
  value?: string;
  field?: string;
  member?: string;
  index?: number;
  expiry?: number;
}

export class RedisSQLite extends EventEmitter {
  private db: DB;
  private transactionMode: boolean = false;
  private transactionCommands: Array<{
    command: string;
    args: Array<string | number>;
  }> = [];
  private password: string | null = null;
  private isAuthenticated: boolean = false;
  private blockingOperations: Map<
    string,
    Array<(value: string | null) => void>
  > = new Map();
  private expiryCheckerId: NodeJS.Timeout | null = null;

  constructor(private options: RedisOptions = {}) {
    super();
    this.password = options.password ?? null;
    const filename = this.options.filename || ":memory:";
    this.db = new DB(filename);
  }

  async connect(): Promise<void> {
    await this.initializeTables();
    this.startExpiryChecker();
  }

  private async initializeTables(): Promise<void> {
    // String store
    await this.db.createTable(
      this.db
        .schema(`${TABLE_PREFIX}string_store`)
        .field("key")
        .type("Text")
        .required()
        .primaryKey()
        .done()
        .field("value")
        .type("Text")
        .done()
        .field("expiry")
        .type("Integer")
        .index()
        .done()
    );

    // Hash store
    await this.db.createTable(
      this.db
        .schema(`${TABLE_PREFIX}hash_store`)
        .field("key")
        .type("Text")
        .required()
        .done()
        .field("field")
        .type("Text")
        .required()
        .done()
        .field("value")
        .type("Text")
        .done()
        .field("expiry")
        .type("Integer")
        .index()
        .done()
        .primaryKey(["key", "field"])
    );

    // List store
    await this.db.createTable(
      this.db
        .schema(`${TABLE_PREFIX}list_store`)
        .field("key")
        .type("Text")
        .required()
        .done()
        .field("index")
        .type("Integer")
        .required()
        .done()
        .field("value")
        .type("Text")
        .done()
        .field("expiry")
        .type("Integer")
        .index()
        .done()
        .primaryKey(["key", "index"])
    );

    // Set store
    await this.db.createTable(
      this.db
        .schema(`${TABLE_PREFIX}set_store`)
        .field("key")
        .type("Text")
        .required()
        .done()
        .field("member")
        .type("Text")
        .required()
        .done()
        .field("expiry")
        .type("Integer")
        .index()
        .done()
        .primaryKey(["key", "member"])
    );
  }

  private startExpiryChecker(): void {
    this.expiryCheckerId = setInterval(async () => {
      const now = Date.now();
      const tables = [
        `${TABLE_PREFIX}string_store`,
        `${TABLE_PREFIX}hash_store`,
        `${TABLE_PREFIX}list_store`,
        `${TABLE_PREFIX}set_store`,
      ];

      for (const table of tables) {
        await this.db.query(table).where("expiry", Cmp.Lt, now).delete();
      }
    }, 1000);
  }

  // Authentication
  async auth(password: string): Promise<"OK"> {
    if (!this.password) {
      throw new Error("ERR Client sent AUTH, but no password is set");
    }

    if (password === this.password) {
      this.isAuthenticated = true;
      return "OK";
    }

    throw new Error("ERR invalid password");
  }

  private checkAuth(): void {
    if (this.password && !this.isAuthenticated) {
      throw new Error("NOAUTH Authentication required.");
    }
  }

  // String operations
  async set(key: string, value: string, db: DB = this.db): Promise<"OK"> {
    const result = (await db
      .query(`${TABLE_PREFIX}string_store`)
      .where("key", Cmp.Eq, key)
      .first()) as Record | undefined;

    if (result) {
      await db.update(`${TABLE_PREFIX}string_store`, result.id, {
        value,
      });
    } else {
      await db.insert(`${TABLE_PREFIX}string_store`, { key, value });
    }

    return "OK";
  }

  async get(key: string, db: DB = this.db): Promise<string | null> {
    const now = Date.now();
    const result = (await db
      .query(`${TABLE_PREFIX}string_store`)
      .where("key", Cmp.Eq, key)
      .and("expiry", Cmp.Gt, now)
      .first()) as Record | undefined;

    return result?.value ?? null;
  }

  async mget(...args: Array<string | DB>): Promise<(string | null)[]> {
    let db: DB = this.db;
    if (args[args.length - 1] instanceof DB) {
      db = args.pop() as DB;
    }
    const keys = args as string[];

    const now = Date.now();
    const results = (await db
      .query(`${TABLE_PREFIX}string_store`)
      .where("key", Cmp.In, keys)
      .and("expiry", Cmp.Gt, now)
      .execute()) as Record[];

    return keys.map((key) => results.find((r) => r.key === key)?.value ?? null);
  }

  // List operations
  async lpush(key: string, ...args: Array<string | DB>): Promise<number> {
    let db: DB = this.db;
    if (args[args.length - 1] instanceof DB) {
      db = args.pop() as DB;
    }
    const values = args as string[];

    // Check for wrong type
    const now = Date.now();
    const stringExists = await db
      .query(`${TABLE_PREFIX}string_store`)
      .where("key", Cmp.Eq, key)
      .and("expiry", Cmp.Gt, now)
      .exists();

    if (stringExists) {
      throw new Error(
        "WRONGTYPE Operation against a key holding the wrong kind of value"
      );
    }

    // Shift existing elements
    const records = (await db
      .query(`${TABLE_PREFIX}list_store`)
      .where("key", Cmp.Eq, key)
      .execute()) as Record[];

    for (const record of records) {
      await db.update(`${TABLE_PREFIX}list_store`, record.id, {
        index: (record.index ?? 0) + values.length,
      });
    }

    // Insert new elements
    for (let i = 0; i < values.length; i++) {
      await db.insert(`${TABLE_PREFIX}list_store`, {
        key,
        index: i,
        value: values[values.length - 1 - i],
      });
    }

    const count = await db
      .query(`${TABLE_PREFIX}list_store`)
      .where("key", Cmp.Eq, key)
      .count();

    return count;
  }

  async rpush(key: string, ...args: Array<string | DB>): Promise<number> {
    let db: DB = this.db;
    if (args[args.length - 1] instanceof DB) {
      db = args.pop() as DB;
    }
    const values = args as string[];

    // Check for wrong type
    const now = Date.now();
    const stringExists = await db
      .query(`${TABLE_PREFIX}string_store`)
      .where("key", Cmp.Eq, key)
      .and("expiry", Cmp.Gt, now)
      .exists();

    if (stringExists) {
      throw new Error(
        "WRONGTYPE Operation against a key holding the wrong kind of value"
      );
    }

    const currentLength = await db
      .query(`${TABLE_PREFIX}list_store`)
      .where("key", Cmp.Eq, key)
      .count();

    for (let i = 0; i < values.length; i++) {
      await db.insert(`${TABLE_PREFIX}list_store`, {
        key,
        index: currentLength + i,
        value: values[i],
      });
    }

    return currentLength + values.length;
  }

  async lrange(
    key: string,
    start: number,
    stop: number,
    db: DB = this.db
  ): Promise<string[]> {
    const length = await db
      .query(`${TABLE_PREFIX}list_store`)
      .where("key", Cmp.Eq, key)
      .count();

    let realStart = start >= 0 ? start : length + start;
    let realStop = stop >= 0 ? stop : length + stop;

    realStart = Math.max(0, realStart);
    realStop = Math.min(length - 1, realStop);

    if (realStart > realStop) {
      return [];
    }

    const results = (await db
      .query(`${TABLE_PREFIX}list_store`)
      .where("key", Cmp.Eq, key)
      .where("index", Cmp.Gte, realStart)
      .where("index", Cmp.Lte, realStop)
      .orderBy("index", "asc")
      .execute()) as Record[];

    return results.map((r) => r.value ?? "");
  }

  async rpop(key: string, db: DB = this.db): Promise<string | null> {
    const result = (await db
      .query(`${TABLE_PREFIX}list_store`)
      .where("key", Cmp.Eq, key)
      .orderBy("index", "desc")
      .first()) as Record | undefined;

    if (result) {
      await db
        .query(`${TABLE_PREFIX}list_store`)
        .where("key", Cmp.Eq, key)
        .where("index", Cmp.Eq, result.index)
        .delete();
      return result.value ?? null;
    }

    return null;
  }

  async lpop(key: string, db: DB = this.db): Promise<string | null> {
    const result = (await db
      .query(`${TABLE_PREFIX}list_store`)
      .where("key", Cmp.Eq, key)
      .orderBy("index", "asc")
      .first()) as Record | undefined;

    if (result) {
      await db
        .query(`${TABLE_PREFIX}list_store`)
        .where("key", Cmp.Eq, key)
        .where("index", Cmp.Eq, 0)
        .delete();

      const results = (await db
        .query(`${TABLE_PREFIX}list_store`)
        .where("key", Cmp.Eq, key)
        .execute()) as Record[];

      for (const record of results) {
        await db.update(`${TABLE_PREFIX}list_store`, record.id, {
          index: (record.index ?? 0) - 1,
        });
      }

      return result.value ?? null;
    }

    return null;
  }

  async rpoplpush(
    source: string,
    destination: string,
    db: DB = this.db
  ): Promise<string | null> {
    const value = await this.rpop(source, db);
    if (value !== null) {
      await this.lpush(destination, value, db);
      return value;
    }
    return null;
  }

  async brpoplpush(
    source: string,
    destination: string,
    timeout: number
  ): Promise<string | null> {
    const value = await this.rpoplpush(source, destination);
    if (value !== null) return value;

    return new Promise((resolve) => {
      const timeoutId = setTimeout(() => {
        this.removeBlockingOperation(source, resolve);
        resolve(null);
      }, timeout * 1000);

      this.addBlockingOperation(source, async (value) => {
        clearTimeout(timeoutId);
        if (value !== null) {
          await this.lpush(destination, value);
        }
        resolve(value);
      });
    });
  }

  private addBlockingOperation(
    key: string,
    resolver: (value: string | null) => void
  ): void {
    if (!this.blockingOperations.has(key)) {
      this.blockingOperations.set(key, []);
    }
    this.blockingOperations.get(key)?.push(resolver);
  }

  private removeBlockingOperation(
    key: string,
    resolver: (value: string | null) => void
  ): void {
    const operations = this.blockingOperations.get(key);
    if (operations) {
      const index = operations.indexOf(resolver);
      if (index !== -1) {
        operations.splice(index, 1);
      }
    }
  }

  // Hash operations
  async hset(
    key: string,
    field: string,
    value: string,
    db: DB = this.db
  ): Promise<number> {
    const result = (await db
      .query(`${TABLE_PREFIX}hash_store`)
      .where("key", Cmp.Eq, key)
      .where("field", Cmp.Eq, field)
      .first()) as Record | undefined;

    if (result) {
      await db.update(`${TABLE_PREFIX}hash_store`, result.id, { value });
    } else {
      await db.insert(`${TABLE_PREFIX}hash_store`, { key, field, value });
    }

    return result ? 1 : 0;
  }

  async hmset(key: string, ...args: Array<string | DB>): Promise<"OK"> {
    let db: DB = this.db;
    if (args[args.length - 1] instanceof DB) {
      db = args.pop() as DB;
    }
    const fieldValues = args as string[];

    if (fieldValues.length % 2 !== 0) {
      throw new Error("Wrong number of arguments for HMSET");
    }

    for (let i = 0; i < fieldValues.length; i += 2) {
      await this.hset(key, fieldValues[i], fieldValues[i + 1], db);
    }

    return "OK";
  }

  async hget(
    key: string,
    field: string,
    db: DB = this.db
  ): Promise<string | null> {
    // Check for wrong type
    const stringExists = await db
      .query(`${TABLE_PREFIX}string_store`)
      .where("key", Cmp.Eq, key)
      .where("expiry", Cmp.Eq, null)
      .or("expiry", Cmp.Gt, Date.now())
      .exists();

    if (stringExists) {
      throw new Error(
        "WRONGTYPE Operation against a key holding the wrong kind of value"
      );
    }

    const result = (await db
      .query(`${TABLE_PREFIX}hash_store`)
      .where("key", Cmp.Eq, key)
      .where("field", Cmp.Eq, field)
      .where("expiry", Cmp.Eq, null)
      .or("expiry", Cmp.Gt, Date.now())
      .first()) as Record | undefined;

    return result?.value ?? null;
  }

  async hdel(key: string, ...args: Array<string | DB>): Promise<number> {
    let db: DB = this.db;
    if (args[args.length - 1] instanceof DB) {
      db = args.pop() as DB;
    }
    const fields = args as string[];

    const result = await db
      .query(`${TABLE_PREFIX}hash_store`)
      .where("key", Cmp.Eq, key)
      .where("field", Cmp.In, fields)
      .delete();

    return result ? fields.length : 0;
  }

  async hmget(
    key: string,
    ...args: Array<string | DB>
  ): Promise<(string | null)[]> {
    let db: DB = this.db;
    if (args[args.length - 1] instanceof DB) {
      db = args.pop() as DB;
    }
    const fields = args as string[];

    const now = Date.now();
    const results = (await db
      .query(`${TABLE_PREFIX}hash_store`)
      .where("key", Cmp.Eq, key)
      .where("field", Cmp.In, fields)
      .where("expiry", Cmp.Eq, null)
      .or("expiry", Cmp.Gt, now)
      .execute()) as Record[];

    return fields.map(
      (field) => results.find((r) => r.field === field)?.value ?? null
    );
  }

  // Set operations
  async sadd(key: string, ...args: Array<string | DB>): Promise<number> {
    let db: DB = this.db;
    if (args[args.length - 1] instanceof DB) {
      db = args.pop() as DB;
    }
    const members = args as string[];

    // Check for wrong type
    const stringExists = await db
      .query(`${TABLE_PREFIX}string_store`)
      .where("key", Cmp.Eq, key)
      .where("expiry", Cmp.Eq, null)
      .or("expiry", Cmp.Gt, Date.now())
      .exists();

    if (stringExists) {
      throw new Error(
        "WRONGTYPE Operation against a key holding the wrong kind of value"
      );
    }

    let added = 0;
    for (const member of members) {
      const result = await db.insert(`${TABLE_PREFIX}set_store`, {
        key,
        member,
      });

      if (result) added++;
    }

    return added;
  }

  async sismember(
    key: string,
    member: string,
    db: DB = this.db
  ): Promise<number> {
    const exists = await db
      .query(`${TABLE_PREFIX}set_store`)
      .where("key", Cmp.Eq, key)
      .where("member", Cmp.Eq, member)
      .where("expiry", Cmp.Eq, null)
      .or("expiry", Cmp.Gt, Date.now())
      .exists();

    return exists ? 1 : 0;
  }

  async smembers(key: string, db: DB = this.db): Promise<string[]> {
    const results = (await db
      .query(`${TABLE_PREFIX}set_store`)
      .where("key", Cmp.Eq, key)
      .where("expiry", Cmp.Eq, null)
      .or("expiry", Cmp.Gt, Date.now())
      .orderBy("member", "asc")
      .execute()) as Record[];

    return results.map((r) => r.member ?? "");
  }

  async srem(key: string, ...args: Array<string | DB>): Promise<number> {
    let db: DB = this.db;
    if (args[args.length - 1] instanceof DB) {
      db = args.pop() as DB;
    }
    const members = args as string[];

    const result = await db
      .query(`${TABLE_PREFIX}set_store`)
      .where("key", Cmp.Eq, key)
      .where("member", Cmp.In, members)
      .delete();

    return result ? members.length : 0;
  }

  // Key operations
  async del(...args: Array<string | DB>): Promise<number> {
    let db: DB = this.db;
    if (args[args.length - 1] instanceof DB) {
      db = args.pop() as DB;
    }
    const keys = args as string[];

    let deleted = 0;
    const tables = ["string_store", "hash_store", "list_store", "set_store"];

    for (const table of tables) {
      for (const key of keys) {
        const result = await db
          .query(`${TABLE_PREFIX}${table}`)
          .where("key", Cmp.Eq, key)
          .delete();

        if (result) deleted++;
      }
    }

    return deleted;
  }

  async exists(...args: Array<string | DB>): Promise<number> {
    let db: DB = this.db;
    if (args[args.length - 1] instanceof DB) {
      db = args.pop() as DB;
    }
    const keys = args as string[];

    let count = 0;
    const tables = ["string_store", "hash_store", "list_store", "set_store"];

    for (const key of keys) {
      for (const table of tables) {
        const exists = await db
          .query(`${TABLE_PREFIX}${table}`)
          .where("key", Cmp.Eq, key)
          .exists();

        if (exists) {
          count++;
          break;
        }
      }
    }

    return count;
  }

  // Key expiration
  async expire(
    key: string,
    seconds: number,
    db: DB = this.db
  ): Promise<number> {
    const expiry = Date.now() + seconds * 1000;
    let updated = 0;

    const tables = ["string_store", "hash_store", "list_store", "set_store"];

    for (const table of tables) {
      const result = (await db
        .query(`${TABLE_PREFIX}${table}`)
        .where("key", Cmp.Eq, key)
        .execute()) as Record[];

      if (result.length > 0) {
        for (const record of result) {
          await db.update(`${TABLE_PREFIX}${table}`, record.id, { expiry });
        }
        updated++;
      }
    }

    return updated > 0 ? 1 : 0;
  }

  async ttl(key: string, db: DB = this.db): Promise<number> {
    const now = Date.now();
    const tables = ["string_store", "hash_store", "list_store", "set_store"];

    for (const table of tables) {
      const result = (await db
        .query(`${TABLE_PREFIX}${table}`)
        .where("key", Cmp.Eq, key)
        .where("expiry", Cmp.Ne, null)
        .first()) as Record | undefined;

      if (result) {
        const ttl = Math.ceil(((result.expiry ?? 0) - now) / 1000);
        return ttl > 0 ? ttl : -1;
      }
    }

    return -2; // Key does not exist
  }

  // Transaction support
  multi(): void {
    this.transactionMode = true;
    this.transactionCommands = [];
  }

  addCommand(command: string, args: Array<string | number>): void {
    if (!this.transactionMode) {
      throw new Error("No transaction in progress");
    }
    this.transactionCommands.push({ command, args });
  }

  async exec(): Promise<Array<[Error | null, string | number | null]>> {
    if (!this.transactionMode) {
      throw new Error("No transaction in progress");
    }

    const results: Array<[Error | null, string | number | null]> = [];
    const tx = await this.db.startTransaction();

    try {
      for (const cmd of this.transactionCommands) {
        try {
          // Check for wrong type errors before executing command
          if (
            cmd.command === "hget" ||
            cmd.command === "hset" ||
            cmd.command === "hmset"
          ) {
            const stringExists = await tx
              .query(`${TABLE_PREFIX}string_store`)
              .where("key", Cmp.Eq, cmd.args[0])
              .where("expiry", Cmp.Eq, null)
              .or("expiry", Cmp.Gt, Date.now())
              .exists();

            if (stringExists) {
              throw new Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
              );
            }
          }

          // Pass the transaction object as the last argument
          const result = await (this as any)[cmd.command](...cmd.args, tx);
          results.push([null, result]);
        } catch (error) {
          const redisError =
            error instanceof Error ? error : new Error(String(error));
          results.push([redisError, null]);
        }
      }

      await tx.commitTransaction();
      this.transactionMode = false;
      this.transactionCommands = [];

      return results;
    } catch (error) {
      await tx.rollbackTransaction();
      this.transactionMode = false;
      this.transactionCommands = [];
      throw error;
    } finally {
      await tx.releaseTransaction();
    }
  }

  // Database operations
  async flushdb(db: DB = this.db): Promise<void> {
    const tables = ["string_store", "hash_store", "list_store", "set_store"];

    for (const table of tables) {
      await db.query(`${TABLE_PREFIX}${table}`).delete();
    }
  }

  async flushall(): Promise<void> {
    return this.flushdb();
  }

  // Server info
  async info(): Promise<{ [key: string]: string }> {
    return {
      redis_version: "6.0.0",
    };
  }

  // Cleanup
  async close(): Promise<void> {
    if (this.expiryCheckerId) {
      clearInterval(this.expiryCheckerId);
      this.expiryCheckerId = null;
    }
  }
}
