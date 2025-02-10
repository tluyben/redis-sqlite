import sqlite3 from "sqlite3";
import { Database, open } from "sqlite";
import { EventEmitter } from "events";

const TABLE_PREFIX = process.env.REDIS_SQLITE_PREFIX || "redis_";

interface RedisOptions {
  filename?: string; // :memory: or file path
  password?: string; // Redis auth password
}

export class RedisSQLite extends EventEmitter {
  private db: Database | null = null;
  private inTransaction: boolean = false;
  private transactionMode: boolean = false;
  private transactionCommands: Array<{
    command: string;
    args: any[];
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
  }

  async connect(): Promise<void> {
    const filename = this.options.filename || ":memory:";
    this.db = await open({
      filename,
      driver: sqlite3.Database,
    });

    await this.initializeTables();
    this.startExpiryChecker();
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

  private async initializeTables(): Promise<void> {
    if (!this.db) throw new Error("Database not initialized");

    // Main key-value store for strings
    await this.db.exec(`
      CREATE TABLE IF NOT EXISTS ${TABLE_PREFIX}string_store (
        key TEXT PRIMARY KEY,
        value TEXT,
        expiry INTEGER
      )`);

    // Hash store (crucial for Bull job data)
    await this.db.exec(`
      CREATE TABLE IF NOT EXISTS ${TABLE_PREFIX}hash_store (
        key TEXT,
        field TEXT,
        value TEXT,
        expiry INTEGER,
        PRIMARY KEY (key, field)
      )`);

    // List store (for job queues)
    await this.db.exec(`
      CREATE TABLE IF NOT EXISTS ${TABLE_PREFIX}list_store (
        key TEXT,
        "index" INTEGER,
        value TEXT,
        expiry INTEGER,
        PRIMARY KEY (key, "index")
      )`);

    // Set store (for unique job IDs and delayed jobs)
    await this.db.exec(`
      CREATE TABLE IF NOT EXISTS ${TABLE_PREFIX}set_store (
        key TEXT,
        member TEXT,
        expiry INTEGER,
        PRIMARY KEY (key, member)
      )`);

    // Create indexes - one statement at a time to avoid syntax errors
    await this.db.exec(
      `CREATE INDEX IF NOT EXISTS idx_${TABLE_PREFIX}string_store_expiry ON ${TABLE_PREFIX}string_store(expiry)`
    );
    await this.db.exec(
      `CREATE INDEX IF NOT EXISTS idx_${TABLE_PREFIX}hash_store_expiry ON ${TABLE_PREFIX}hash_store(expiry)`
    );
    await this.db.exec(
      `CREATE INDEX IF NOT EXISTS idx_${TABLE_PREFIX}list_store_expiry ON ${TABLE_PREFIX}list_store(expiry)`
    );
    await this.db.exec(
      `CREATE INDEX IF NOT EXISTS idx_${TABLE_PREFIX}set_store_expiry ON ${TABLE_PREFIX}set_store(expiry)`
    );
  }

  private startExpiryChecker(): void {
    this.expiryCheckerId = setInterval(async () => {
      if (!this.db) return;
      const now = Date.now();

      const tables = [
        `${TABLE_PREFIX}string_store`,
        `${TABLE_PREFIX}hash_store`,
        `${TABLE_PREFIX}list_store`,
        `${TABLE_PREFIX}set_store`,
      ];
      for (const table of tables) {
        await this.db.run(`DELETE FROM ${table} WHERE expiry < ?`, now);
      }
    }, 1000); // Check every second
  }

  // String operations
  async set(key: string, value: string): Promise<"OK"> {
    if (!this.db) throw new Error("Database not initialized");

    await this.db.run(
      `INSERT OR REPLACE INTO ${TABLE_PREFIX}string_store (key, value) VALUES (?, ?)`,
      [key, value]
    );

    return "OK";
  }

  async get(key: string): Promise<string | null> {
    if (!this.db) throw new Error("Database not initialized");

    const result = await this.db.get<{ value: string }>(
      `SELECT value FROM ${TABLE_PREFIX}string_store WHERE key = ? AND (expiry IS NULL OR expiry > ?)`,
      [key, Date.now()]
    );

    return result ? result.value : null;
  }

  async mget(...keys: string[]): Promise<(string | null)[]> {
    if (!this.db) throw new Error("Database not initialized");

    const results: (string | null)[] = [];
    for (const key of keys) {
      const result = await this.get(key);
      results.push(result);
    }
    return results;
  }

  // List operations (crucial for Bull)
  async lpush(key: string, ...values: string[]): Promise<number> {
    if (!this.db) throw new Error("Database not initialized");

    // Check if key exists in string_store
    const stringResult = await this.db.get(
      `SELECT 1 FROM ${TABLE_PREFIX}string_store WHERE key = ? AND (expiry IS NULL OR expiry > ?)`,
      [key, Date.now()]
    );

    if (stringResult) {
      throw new Error(
        "WRONGTYPE Operation against a key holding the wrong kind of value"
      );
    }

    if (!this.inTransaction && !this.transactionMode) {
      await this.db.run("BEGIN TRANSACTION");
      this.inTransaction = true;
    }
    try {
      // Shift existing elements
      await this.db.run(
        `UPDATE ${TABLE_PREFIX}list_store SET "index" = "index" + ? WHERE key = ?`,
        [values.length, key]
      );

      // Insert new elements in reverse order for LIFO behavior
      for (let i = 0; i < values.length; i++) {
        await this.db.run(
          `INSERT INTO ${TABLE_PREFIX}list_store (key, "index", value) VALUES (?, ?, ?)`,
          [key, i, values[values.length - 1 - i]]
        );
      }

      if (!this.inTransaction && !this.transactionMode) {
        await this.db.run("COMMIT");
        this.inTransaction = false;
      }

      const result = await this.db.get<{ count: number }>(
        `SELECT COUNT(*) as count FROM ${TABLE_PREFIX}list_store WHERE key = ?`,
        [key]
      );
      return result?.count ?? 0;
    } catch (error) {
      if (!this.inTransaction && !this.transactionMode) {
        await this.db.run("ROLLBACK");
        this.inTransaction = false;
      }
      throw error;
    }
  }

  async rpush(key: string, ...values: string[]): Promise<number> {
    if (!this.db) throw new Error("Database not initialized");

    // Check if key exists in string_store
    const stringResult = await this.db.get(
      `SELECT 1 FROM ${TABLE_PREFIX}string_store WHERE key = ? AND (expiry IS NULL OR expiry > ?)`,
      [key, Date.now()]
    );

    if (stringResult) {
      throw new Error(
        "WRONGTYPE Operation against a key holding the wrong kind of value"
      );
    }

    const result = await this.db.get<{ count: number }>(
      `SELECT COUNT(*) as count FROM ${TABLE_PREFIX}list_store WHERE key = ?`,
      [key]
    );
    let currentLength = result?.count ?? 0;

    if (!this.inTransaction && !this.transactionMode) {
      await this.db.run("BEGIN TRANSACTION");
      this.inTransaction = true;
    }

    try {
      for (const value of values) {
        await this.db.run(
          `INSERT INTO ${TABLE_PREFIX}list_store (key, "index", value) VALUES (?, ?, ?)`,
          [key, currentLength++, value]
        );
      }

      if (!this.inTransaction && !this.transactionMode) {
        await this.db.run("COMMIT");
        this.inTransaction = false;
      }

      return currentLength;
    } catch (error) {
      if (!this.inTransaction && !this.transactionMode) {
        await this.db.run("ROLLBACK");
        this.inTransaction = false;
      }
      throw error;
    }
  }

  async lrange(key: string, start: number, stop: number): Promise<string[]> {
    if (!this.db) throw new Error("Database not initialized");

    // Convert negative indices to positive ones
    const result = await this.db.get<{ count: number }>(
      `SELECT COUNT(*) as count FROM ${TABLE_PREFIX}list_store WHERE key = ?`,
      [key]
    );
    const length = result?.count ?? 0;

    let realStart = start >= 0 ? start : length + start;
    let realStop = stop >= 0 ? stop : length + stop;

    // Ensure indices are within bounds
    realStart = Math.max(0, realStart);
    realStop = Math.min(length - 1, realStop);

    if (realStart > realStop) {
      return [];
    }

    const results = await this.db.all<{ value: string }[]>(
      `SELECT value FROM ${TABLE_PREFIX}list_store WHERE key = ? AND "index" BETWEEN ? AND ? ORDER BY "index" ASC`,
      [key, realStart, realStop]
    );

    return results.map((row) => row.value);
  }

  async rpop(
    key: string,
    inTransaction: boolean = false
  ): Promise<string | null> {
    if (!this.db) throw new Error("Database not initialized");

    const result = await this.db.get<{ value: string; index: number }>(
      `SELECT value, "index" FROM ${TABLE_PREFIX}list_store WHERE key = ? ORDER BY "index" DESC LIMIT 1`,
      [key]
    );

    if (result) {
      if (!inTransaction) {
        await this.db.run("BEGIN TRANSACTION");
        this.inTransaction = true;
      }
      try {
        await this.db.run(
          `DELETE FROM ${TABLE_PREFIX}list_store WHERE key = ? AND "index" = ?`,
          [key, result.index]
        );
        if (!inTransaction) {
          await this.db.run("COMMIT");
          this.inTransaction = false;
        }
        return result.value;
      } catch (error) {
        if (!inTransaction) {
          await this.db.run("ROLLBACK");
          this.inTransaction = false;
        }
        throw error;
      }
    }

    return null;
  }

  async lpop(key: string): Promise<string | null> {
    if (!this.db) throw new Error("Database not initialized");

    const result = await this.db.get<{ value: string }>(
      `SELECT value FROM ${TABLE_PREFIX}list_store WHERE key = ? ORDER BY "index" ASC LIMIT 1`,
      [key]
    );

    if (result) {
      if (!this.inTransaction && !this.transactionMode) {
        await this.db.run("BEGIN TRANSACTION");
        this.inTransaction = true;
      }

      try {
        await this.db.run(
          `DELETE FROM ${TABLE_PREFIX}list_store WHERE key = ? AND "index" = 0`,
          [key]
        );
        await this.db.run(
          `UPDATE ${TABLE_PREFIX}list_store SET "index" = "index" - 1 WHERE key = ?`,
          [key]
        );

        if (!this.inTransaction && !this.transactionMode) {
          await this.db.run("COMMIT");
          this.inTransaction = false;
        }
        return result.value;
      } catch (error) {
        if (!this.inTransaction && !this.transactionMode) {
          await this.db.run("ROLLBACK");
          this.inTransaction = false;
        }
        throw error;
      }
    }

    return null;
  }

  // Critical for Bull: RPOPLPUSH and BRPOPLPUSH
  async rpoplpush(source: string, destination: string): Promise<string | null> {
    if (!this.db) throw new Error("Database not initialized");

    if (!this.inTransaction && !this.transactionMode) {
      await this.db.run("BEGIN TRANSACTION");
      this.inTransaction = true;
    }

    try {
      const value = await this.rpop(source, true);
      if (value !== null) {
        await this.lpush(destination, value);
        if (!this.inTransaction && !this.transactionMode) {
          await this.db.run("COMMIT");
          this.inTransaction = false;
        }
        return value;
      }
      if (!this.inTransaction && !this.transactionMode) {
        await this.db.run("ROLLBACK");
        this.inTransaction = false;
      }
      return null;
    } catch (error) {
      if (!this.inTransaction && !this.transactionMode) {
        await this.db.run("ROLLBACK");
        this.inTransaction = false;
      }
      throw error;
    }
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
  async hset(key: string, field: string, value: string): Promise<number> {
    if (!this.db) throw new Error("Database not initialized");

    const result = await this.db.run(
      `INSERT OR REPLACE INTO ${TABLE_PREFIX}hash_store (key, field, value) VALUES (?, ?, ?)`,
      [key, field, value]
    );

    return result.changes ?? 0;
  }

  async hmset(key: string, ...fieldValues: string[]): Promise<"OK"> {
    if (!this.db) throw new Error("Database not initialized");
    if (fieldValues.length % 2 !== 0)
      throw new Error("Wrong number of arguments for HMSET");

    await this.db.run("BEGIN TRANSACTION");
    try {
      for (let i = 0; i < fieldValues.length; i += 2) {
        await this.hset(key, fieldValues[i], fieldValues[i + 1]);
      }
      await this.db.run("COMMIT");
      return "OK";
    } catch (error) {
      await this.db.run("ROLLBACK");
      throw error;
    }
  }

  async hget(key: string, field: string): Promise<string | null> {
    if (!this.db) throw new Error("Database not initialized");

    // Check if key exists in string_store
    const stringResult = await this.db.get(
      `SELECT 1 FROM ${TABLE_PREFIX}string_store WHERE key = ? AND (expiry IS NULL OR expiry > ?)`,
      [key, Date.now()]
    );

    if (stringResult) {
      throw new Error(
        "WRONGTYPE Operation against a key holding the wrong kind of value"
      );
    }

    const result = await this.db.get<{ value: string }>(
      `SELECT value FROM ${TABLE_PREFIX}hash_store WHERE key = ? AND field = ? AND (expiry IS NULL OR expiry > ?)`,
      [key, field, Date.now()]
    );

    return result ? result.value : null;
  }

  async hdel(key: string, ...fields: string[]): Promise<number> {
    if (!this.db) throw new Error("Database not initialized");

    const result = await this.db.run(
      `DELETE FROM ${TABLE_PREFIX}hash_store WHERE key = ? AND field IN (` +
        fields.map(() => "?").join(",") +
        ")",
      [key, ...fields]
    );

    return result.changes ?? 0;
  }

  // Key operations
  async del(...keys: string[]): Promise<number> {
    if (!this.db) throw new Error("Database not initialized");

    let deleted = 0;
    await this.db.run("BEGIN TRANSACTION");
    try {
      const tables = ["string_store", "hash_store", "list_store", "set_store"];
      for (const table of tables) {
        for (const key of keys) {
          const result = await this.db.run(
            `DELETE FROM ${TABLE_PREFIX}${table} WHERE key = ?`,
            [key]
          );
          deleted += (result.changes ?? 0) > 0 ? 1 : 0;
        }
      }
      await this.db.run("COMMIT");
      return deleted;
    } catch (error) {
      await this.db.run("ROLLBACK");
      throw error;
    }
  }

  async exists(...keys: string[]): Promise<number> {
    if (!this.db) throw new Error("Database not initialized");

    let count = 0;
    const tables = ["string_store", "hash_store", "list_store", "set_store"];

    for (const key of keys) {
      let exists = false;
      for (const table of tables) {
        const result = await this.db.get(
          `SELECT 1 FROM ${TABLE_PREFIX}${table} WHERE key = ? LIMIT 1`,
          [key]
        );
        if (result) {
          exists = true;
          break;
        }
      }
      if (exists) count++;
    }

    return count;
  }

  // Hash operations additions
  async hmget(key: string, ...fields: string[]): Promise<(string | null)[]> {
    if (!this.db) throw new Error("Database not initialized");

    const results: (string | null)[] = [];
    for (const field of fields) {
      const result = await this.db.get<{ value: string }>(
        `SELECT value FROM ${TABLE_PREFIX}hash_store WHERE key = ? AND field = ? AND (expiry IS NULL OR expiry > ?)`,
        [key, field, Date.now()]
      );
      results.push(result ? result.value : null);
    }

    return results;
  }

  // Set operations additions
  async sismember(key: string, member: string): Promise<number> {
    if (!this.db) throw new Error("Database not initialized");

    const result = await this.db.get(
      `SELECT 1 FROM ${TABLE_PREFIX}set_store WHERE key = ? AND member = ? AND (expiry IS NULL OR expiry > ?)`,
      [key, member, Date.now()]
    );

    return result ? 1 : 0;
  }

  async smembers(key: string): Promise<string[]> {
    if (!this.db) throw new Error("Database not initialized");

    const results = await this.db.all<{ member: string }[]>(
      `SELECT member FROM ${TABLE_PREFIX}set_store WHERE key = ? AND (expiry IS NULL OR expiry > ?) ORDER BY member`,
      [key, Date.now()]
    );

    return results.map((row) => row.member);
  }

  // Set operations
  async sadd(key: string, ...members: string[]): Promise<number> {
    if (!this.db) throw new Error("Database not initialized");

    // Check if key exists in string_store
    const stringResult = await this.db.get(
      `SELECT 1 FROM ${TABLE_PREFIX}string_store WHERE key = ? AND (expiry IS NULL OR expiry > ?)`,
      [key, Date.now()]
    );

    if (stringResult) {
      throw new Error(
        "WRONGTYPE Operation against a key holding the wrong kind of value"
      );
    }

    let added = 0;
    if (!this.inTransaction && !this.transactionMode) {
      await this.db.run("BEGIN TRANSACTION");
      this.inTransaction = true;
    }

    try {
      for (const member of members) {
        const result = await this.db.run(
          `INSERT OR IGNORE INTO ${TABLE_PREFIX}set_store (key, member) VALUES (?, ?)`,
          [key, member]
        );
        added += result.changes ?? 0;
      }

      if (!this.inTransaction && !this.transactionMode) {
        await this.db.run("COMMIT");
        this.inTransaction = false;
      }

      return added;
    } catch (error) {
      if (!this.inTransaction && !this.transactionMode) {
        await this.db.run("ROLLBACK");
        this.inTransaction = false;
      }
      throw error;
    }
  }

  async srem(key: string, ...members: string[]): Promise<number> {
    if (!this.db) throw new Error("Database not initialized");

    const result = await this.db.run(
      `DELETE FROM ${TABLE_PREFIX}set_store WHERE key = ? AND member IN (` +
        members.map(() => "?").join(",") +
        ")",
      [key, ...members]
    );

    return result.changes ?? 0;
  }

  // Transaction support
  multi(): void {
    this.transactionMode = true;
    this.transactionCommands = [];
  }

  addCommand(command: string, args: any[]): void {
    if (!this.transactionMode) {
      throw new Error("No transaction in progress");
    }
    this.transactionCommands.push({ command, args });
  }

  async exec(): Promise<[Error | null, any][]> {
    if (!this.db) throw new Error("Database not initialized");

    if (!this.transactionMode) {
      throw new Error("No transaction in progress");
    }

    const results: [Error | null, any][] = [];

    try {
      if (!this.inTransaction) {
        await this.db.run("BEGIN TRANSACTION");
        this.inTransaction = true;
      }

      for (const cmd of this.transactionCommands) {
        try {
          // Check for wrong type errors before executing command
          if (
            cmd.command === "hget" ||
            cmd.command === "hset" ||
            cmd.command === "hmset"
          ) {
            const stringResult = await this.db.get(
              `SELECT 1 FROM ${TABLE_PREFIX}string_store WHERE key = ? AND (expiry IS NULL OR expiry > ?)`,
              [cmd.args[0], Date.now()]
            );
            if (stringResult) {
              throw new Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
              );
            }
          }

          // Execute the command and collect result
          const result = await (this as any)[cmd.command](...cmd.args);
          results.push([null, result]);
        } catch (error) {
          const redisError =
            error instanceof Error ? error : new Error(String(error));
          results.push([redisError, null]);
        }
      }

      await this.db.run("COMMIT");
      this.inTransaction = false;
      this.transactionMode = false;
      this.transactionCommands = [];

      return results;
    } catch (error) {
      if (this.inTransaction) {
        await this.db.run("ROLLBACK");
        this.inTransaction = false;
      }
      this.transactionMode = false;
      this.transactionCommands = [];
      throw error;
    }
  }

  // Key expiration
  async expire(key: string, seconds: number): Promise<number> {
    if (!this.db) throw new Error("Database not initialized");

    const expiry = Date.now() + seconds * 1000;
    const tables = ["string_store", "hash_store", "list_store", "set_store"];
    let updated = 0;

    for (const table of tables) {
      const result = await this.db.run(
        `UPDATE ${TABLE_PREFIX}${table} SET expiry = ? WHERE key = ?`,
        [expiry, key]
      );
      updated += result.changes ?? 0;
    }

    return updated > 0 ? 1 : 0;
  }

  async ttl(key: string): Promise<number> {
    if (!this.db) throw new Error("Database not initialized");

    const now = Date.now();
    const tables = ["string_store", "hash_store", "list_store", "set_store"];

    for (const table of tables) {
      const result = await this.db.get<{ expiry: number }>(
        `SELECT expiry FROM ${TABLE_PREFIX}${table} WHERE key = ? AND expiry IS NOT NULL LIMIT 1`,
        [key]
      );
      if (result) {
        const ttl = Math.ceil((result.expiry - now) / 1000);
        return ttl > 0 ? ttl : -1;
      }
    }

    return -2; // Key does not exist
  }

  // Database operations
  async flushdb(): Promise<void> {
    if (!this.db) throw new Error("Database not initialized");

    const tables = ["string_store", "hash_store", "list_store", "set_store"];
    await this.db.run("BEGIN TRANSACTION");
    try {
      for (const table of tables) {
        await this.db.run(`DELETE FROM ${TABLE_PREFIX}${table}`);
      }
      await this.db.run("COMMIT");
    } catch (error) {
      await this.db.run("ROLLBACK");
      throw error;
    }
  }

  async flushall(): Promise<void> {
    // In our implementation, flushdb and flushall do the same thing
    // since we don't support multiple databases
    return this.flushdb();
  }

  // Server info
  async info(): Promise<{ [key: string]: string }> {
    return {
      redis_version: "6.0.0", // Hardcoded version for our SQLite-based Redis implementation
    };
  }

  // Cleanup
  async close(): Promise<void> {
    if (this.expiryCheckerId) {
      clearInterval(this.expiryCheckerId);
      this.expiryCheckerId = null;
    }
    if (this.db) {
      await this.db.close();
      this.db = null;
    }
  }
}
