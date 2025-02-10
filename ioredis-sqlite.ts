import { EventEmitter } from "events";
import { RedisSQLite } from "./redis-sqlite";

class RedisError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "RedisError";
  }
}

interface IORedisOptions {
  host?: string;
  port?: number;
  path?: string;
  filename?: string;
  password?: string;
  username?: string; // For Redis 6+ ACL support
  connectTimeout?: number;
  commandTimeout?: number;
  retryStrategy?: (times: number) => number | void | null;
  maxRetriesPerRequest?: number;
  [key: string]: any;
}

interface RedisCommand {
  command: string;
  args: any[];
  resolve: (value: any) => void;
  reject: (error: Error) => void;
}

export class Redis extends EventEmitter {
  private client: RedisSQLite;
  private _isReady: boolean = false;
  private _isConnected: boolean = false;
  status: string = "wait";

  private options: IORedisOptions;

  constructor(options?: IORedisOptions) {
    super();
    this.options = options || {};
    this.client = new RedisSQLite({
      filename: this.options.filename || ":memory:",
      password: this.options.password,
    });

    this.connect();
  }

  private async connect() {
    try {
      await this.client.connect();

      // Handle authentication if password is provided
      if (this.options?.password) {
        await this.auth(this.options.password);
      }

      this._isReady = true;
      this._isConnected = true;
      this.status = "ready";
      this.emit("connect");
      this.emit("ready");
    } catch (error) {
      this.emit("error", error);
      throw error;
    }
  }

  async auth(password: string): Promise<"OK"> {
    return this.client.auth(password);
  }

  // Status checks (ioredis interface)
  get isReady(): boolean {
    return this._isReady;
  }

  get connected(): boolean {
    return this._isConnected;
  }

  // Connection management
  disconnect(): Promise<void> {
    return this.quit();
  }

  async quit(): Promise<void> {
    this._isReady = false;
    this._isConnected = false;
    this.status = "end";
    await this.client.close();
    this.emit("end");
  }

  // Key commands
  async exists(key: string | string[]): Promise<number> {
    const keys = Array.isArray(key) ? key : [key];
    return this.client.exists(...keys);
  }

  async del(key: string | string[]): Promise<number> {
    const keys = Array.isArray(key) ? key : [key];
    return this.client.del(...keys);
  }

  // String commands
  async set(
    key: string,
    value: string | number | Buffer,
    ...args: any[]
  ): Promise<"OK" | null> {
    const stringValue = value.toString();

    // Handle SET options (PX, EX, NX, XX)
    let expireTime: number | undefined;
    for (let i = 0; i < args.length; i += 2) {
      const opt = args[i]?.toString().toUpperCase();
      switch (opt) {
        case "EX":
          expireTime = parseInt(args[i + 1]) * 1000;
          break;
        case "PX":
          expireTime = parseInt(args[i + 1]);
          break;
        // TODO: Implement NX, XX options if needed
      }
    }

    await this.client.set(key, stringValue);
    if (expireTime) {
      await this.client.expire(key, expireTime / 1000);
    }
    return "OK";
  }

  async get(key: string): Promise<string | null> {
    return this.client.get(key);
  }

  // Hash commands
  async hset(
    key: string,
    field: string,
    value: string | number | Buffer
  ): Promise<number>;
  async hset(
    key: string,
    hash: Record<string, string | number | Buffer>
  ): Promise<number>;
  async hset(
    key: string,
    fieldOrHash: string | Record<string, string | number | Buffer>,
    value?: string | number | Buffer
  ): Promise<number> {
    if (typeof fieldOrHash === "string" && value !== undefined) {
      return this.client.hset(key, fieldOrHash, value.toString());
    } else if (typeof fieldOrHash === "object") {
      const fields = Object.entries(fieldOrHash).flat();
      await this.client.hmset(key, ...fields.map((f) => f.toString()));
      return Object.keys(fieldOrHash).length;
    }
    throw new Error("Invalid HSET arguments");
  }

  async hget(key: string, field: string): Promise<string | null> {
    return this.client.hget(key, field);
  }

  async hdel(key: string, ...fields: string[]): Promise<number> {
    return this.client.hdel(key, ...fields);
  }

  async hmset(
    key: string,
    hash: Record<string, string | number | Buffer>
  ): Promise<"OK">;
  async hmset(
    key: string,
    ...args: (string | number | Buffer)[]
  ): Promise<"OK">;
  async hmset(
    key: string,
    hashOrField:
      | Record<string, string | number | Buffer>
      | string
      | number
      | Buffer,
    ...args: (string | number | Buffer)[]
  ): Promise<"OK"> {
    if (typeof hashOrField === "object" && !Buffer.isBuffer(hashOrField)) {
      const fields = Object.entries(hashOrField).flat();
      return this.client.hmset(key, ...fields.map((f) => f.toString()));
    } else {
      return this.client.hmset(
        key,
        hashOrField.toString(),
        ...args.map((a) => a.toString())
      );
    }
  }

  async hmget(key: string, ...fields: string[]): Promise<(string | null)[]> {
    return this.client.hmget(key, ...fields);
  }

  // List commands
  async lpush(
    key: string,
    ...values: (string | number | Buffer)[]
  ): Promise<number> {
    return this.client.lpush(key, ...values.map((v) => v.toString()));
  }

  async rpush(
    key: string,
    ...values: (string | number | Buffer)[]
  ): Promise<number> {
    return this.client.rpush(key, ...values.map((v) => v.toString()));
  }

  async lpop(key: string): Promise<string | null> {
    return this.client.lpop(key);
  }

  async rpop(key: string): Promise<string | null> {
    return this.client.rpop(key);
  }

  async rpoplpush(source: string, destination: string): Promise<string | null> {
    return this.client.rpoplpush(source, destination);
  }

  async brpoplpush(
    source: string,
    destination: string,
    timeout: number
  ): Promise<string | null> {
    return this.client.brpoplpush(source, destination, timeout);
  }

  // Set commands
  async sadd(
    key: string,
    ...members: (string | number | Buffer)[]
  ): Promise<number> {
    return this.client.sadd(key, ...members.map((m) => m.toString()));
  }

  async srem(
    key: string,
    ...members: (string | number | Buffer)[]
  ): Promise<number> {
    return this.client.srem(key, ...members.map((m) => m.toString()));
  }

  async sismember(
    key: string,
    member: string | number | Buffer
  ): Promise<number> {
    return this.client.sismember(key, member.toString());
  }

  async smembers(key: string): Promise<string[]> {
    return this.client.smembers(key);
  }

  // Key expiration
  async expire(key: string, seconds: number): Promise<number> {
    return this.client.expire(key, seconds);
  }

  async ttl(key: string): Promise<number> {
    return this.client.ttl(key);
  }

  // Transaction support
  multi(commands?: Array<[string, ...any[]]>): Pipeline {
    const pipeline = new Pipeline(this);
    this.client.multi();

    if (commands) {
      for (const [command, ...args] of commands) {
        // @ts-ignore - we know these methods exist
        pipeline[command.toLowerCase()](...args);
      }
    }

    return pipeline;
  }

  // Watch-related methods (needed for Bull)
  watch(...keys: string[]): Promise<"OK"> {
    // SQLite doesn't need watch as it has built-in transaction support
    return Promise.resolve("OK");
  }

  unwatch(): Promise<"OK"> {
    // SQLite doesn't need unwatch as it has built-in transaction support
    return Promise.resolve("OK");
  }
}

class Pipeline {
  private commands: Array<{
    command: string;
    args: any[];
    resolve: (value: any) => void;
    reject: (error: Error) => void;
  }> = [];

  constructor(private redis: Redis) {}

  async exec(): Promise<Array<[Error | null, any]>> {
    const results: Array<[Error | null, any]> = [];

    for (const cmd of this.commands) {
      try {
        // @ts-ignore - we know these methods exist on redis
        const result = await this.redis[cmd.command](...cmd.args);
        results.push([null, result]);
        cmd.resolve(result);
      } catch (error) {
        results.push([error as Error, null]);
        cmd.reject(error as Error);
      }
    }

    this.commands = [];
    return results;
  }

  // String operations
  set(key: string, value: string | number | Buffer, ...args: any[]): Pipeline {
    return this.addCommand("set", [key, value, ...args]);
  }

  get(key: string): Pipeline {
    return this.addCommand("get", [key]);
  }

  // Hash operations
  hset(
    key: string,
    field: string | Record<string, any>,
    value?: string | number | Buffer
  ): Pipeline {
    if (typeof field === "object") {
      return this.addCommand("hset", [key, field]);
    }
    return this.addCommand("hset", [key, field, value]);
  }

  hget(key: string, field: string): Pipeline {
    return this.addCommand("hget", [key, field]);
  }

  hdel(key: string, ...fields: string[]): Pipeline {
    return this.addCommand("hdel", [key, ...fields]);
  }

  hmset(key: string, hash: Record<string, any>): Pipeline {
    return this.addCommand("hmset", [key, hash]);
  }

  hmget(key: string, ...fields: string[]): Pipeline {
    return this.addCommand("hmget", [key, ...fields]);
  }

  // List operations
  lpush(key: string, ...values: (string | number | Buffer)[]): Pipeline {
    return this.addCommand("lpush", [key, ...values]);
  }

  rpush(key: string, ...values: (string | number | Buffer)[]): Pipeline {
    return this.addCommand("rpush", [key, ...values]);
  }

  lpop(key: string): Pipeline {
    return this.addCommand("lpop", [key]);
  }

  rpop(key: string): Pipeline {
    return this.addCommand("rpop", [key]);
  }

  rpoplpush(source: string, destination: string): Pipeline {
    return this.addCommand("rpoplpush", [source, destination]);
  }

  brpoplpush(source: string, destination: string, timeout: number): Pipeline {
    return this.addCommand("brpoplpush", [source, destination, timeout]);
  }

  // Set operations
  sadd(key: string, ...members: (string | number | Buffer)[]): Pipeline {
    return this.addCommand("sadd", [key, ...members]);
  }

  srem(key: string, ...members: (string | number | Buffer)[]): Pipeline {
    return this.addCommand("srem", [key, ...members]);
  }

  sismember(key: string, member: string | number | Buffer): Pipeline {
    return this.addCommand("sismember", [key, member]);
  }

  smembers(key: string): Pipeline {
    return this.addCommand("smembers", [key]);
  }

  // Key operations
  del(key: string | string[]): Pipeline {
    const keys = Array.isArray(key) ? key : [key];
    return this.addCommand("del", keys);
  }

  exists(key: string | string[]): Pipeline {
    const keys = Array.isArray(key) ? key : [key];
    return this.addCommand("exists", keys);
  }

  // Expiration operations
  expire(key: string, seconds: number): Pipeline {
    return this.addCommand("expire", [key, seconds]);
  }

  ttl(key: string): Pipeline {
    return this.addCommand("ttl", [key]);
  }

  private addCommand(command: string, args: any[]): Pipeline {
    this.commands.push({
      command,
      args,
      resolve: () => {},
      reject: () => {},
    });
    return this;
  }
}

// Export default and named
export default Redis;
export { Redis as IORedis };
