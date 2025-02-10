import { EventEmitter } from "events";
import { RedisSQLite } from "./redis-sqlite";

interface RedisClientOptions {
  socket?: {
    host?: string;
    port?: number;
    path?: string;
  };
  database?: number;
  username?: string;
  password?: string;
  name?: string;
  filename?: string;
  [key: string]: any;
}

interface AuthOptions {
  username?: string;
  password: string;
}

interface RedisModules {
  [key: string]: any;
}

interface RedisFunctions {
  [key: string]: any;
}

interface RedisScripts {
  [key: string]: any;
}

class RedisClientError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "RedisClientError";
  }
}

class CommandOptions {
  readonly isCommandOptions = true;
  constructor(public readonly returnBuffers?: boolean) {}
}

class RedisClient extends EventEmitter {
  private client: RedisSQLite;
  private _isReady: boolean = false;
  private _isConnected: boolean = false;
  isOpen: boolean = false;

  private options: RedisClientOptions;

  constructor(options?: RedisClientOptions) {
    super();
    this.options = options || {};
    this.client = new RedisSQLite({
      filename: this.options.filename || ":memory:",
      password: this.options.password,
    });
  }

  // Connection management
  async connect(): Promise<void> {
    try {
      await this.client.connect();

      // Handle authentication if password is provided
      if (this.options?.password) {
        await this.auth({ password: this.options.password });
      }

      this._isReady = true;
      this._isConnected = true;
      this.isOpen = true;
      this.emit("connect");
      this.emit("ready");
    } catch (error) {
      this.emit("error", error);
      throw error;
    }
  }

  async auth(options: AuthOptions): Promise<"OK"> {
    return this.client.auth(options.password);
  }

  async disconnect(): Promise<void> {
    this._isReady = false;
    this._isConnected = false;
    this.isOpen = false;
    await this.client.close();
    this.emit("end");
  }

  async quit(): Promise<void> {
    return this.disconnect();
  }

  // String commands
  async set(
    key: string,
    value: string | number | Buffer,
    options?: CommandOptions
  ): Promise<"OK"> {
    return this.client.set(key, value.toString());
  }

  async get(key: string, options?: CommandOptions): Promise<string | null> {
    return this.client.get(key);
  }

  // Hash commands
  async hSet(
    key: string,
    field: string,
    value: string | number | Buffer
  ): Promise<number>;
  async hSet(
    key: string,
    hash: Record<string, string | number | Buffer>
  ): Promise<number>;
  async hSet(
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
    throw new RedisClientError("Invalid HSET arguments");
  }

  async hGet(
    key: string,
    field: string,
    options?: CommandOptions
  ): Promise<string | null> {
    return this.client.hget(key, field);
  }

  async hDel(key: string, ...fields: string[]): Promise<number> {
    return this.client.hdel(key, ...fields);
  }

  async hGetAll(key: string): Promise<Record<string, string>> {
    const fields = await this.client.smembers(`${key}:fields`);
    const result: Record<string, string> = {};

    for (const field of fields) {
      const value = await this.client.hget(key, field);
      if (value !== null) {
        result[field] = value;
      }
    }

    return result;
  }

  // List commands
  async lPush(
    key: string,
    ...values: (string | number | Buffer)[]
  ): Promise<number> {
    return this.client.lpush(key, ...values.map((v) => v.toString()));
  }

  async rPush(
    key: string,
    ...values: (string | number | Buffer)[]
  ): Promise<number> {
    return this.client.rpush(key, ...values.map((v) => v.toString()));
  }

  async lPop(key: string, options?: CommandOptions): Promise<string | null> {
    return this.client.lpop(key);
  }

  async rPop(key: string, options?: CommandOptions): Promise<string | null> {
    return this.client.rpop(key);
  }

  async rPopLPush(source: string, destination: string): Promise<string | null> {
    return this.client.rpoplpush(source, destination);
  }

  async bRPopLPush(
    source: string,
    destination: string,
    timeout: number
  ): Promise<string | null> {
    return this.client.brpoplpush(source, destination, timeout);
  }

  // Set commands
  async sAdd(
    key: string,
    ...members: (string | number | Buffer)[]
  ): Promise<number> {
    return this.client.sadd(key, ...members.map((m) => m.toString()));
  }

  async sRem(
    key: string,
    ...members: (string | number | Buffer)[]
  ): Promise<number> {
    return this.client.srem(key, ...members.map((m) => m.toString()));
  }

  async sIsMember(
    key: string,
    member: string | number | Buffer
  ): Promise<number> {
    return this.client.sismember(key, member.toString());
  }

  async sMembers(key: string): Promise<string[]> {
    return this.client.smembers(key);
  }

  // Key operations
  async del(key: string | string[]): Promise<number> {
    const keys = Array.isArray(key) ? key : [key];
    return this.client.del(...keys);
  }

  async exists(key: string | string[]): Promise<number> {
    const keys = Array.isArray(key) ? key : [key];
    return this.client.exists(...keys);
  }

  // Expiration operations
  async expire(key: string, seconds: number): Promise<number> {
    return this.client.expire(key, seconds);
  }

  async ttl(key: string): Promise<number> {
    return this.client.ttl(key);
  }

  // Multi/Transaction support
  multi(): Multi {
    return new Multi(this);
  }

  // Factory method (node-redis v4 style)
  static async createClient(
    options?: RedisClientOptions
  ): Promise<RedisClient> {
    const client = new RedisClient(options);
    await client.connect();
    return client;
  }
}

class Multi {
  private commands: Array<{
    command: string;
    args: any[];
    resolve: (value: any) => void;
    reject: (error: Error) => void;
  }> = [];

  constructor(private client: RedisClient) {}

  async exec(): Promise<any[]> {
    const results: any[] = [];

    for (const cmd of this.commands) {
      try {
        // @ts-ignore - we know these methods exist on client
        const result = await this.client[cmd.command](...cmd.args);
        results.push(result);
        cmd.resolve(result);
      } catch (error) {
        cmd.reject(error as Error);
        throw error;
      }
    }

    this.commands = [];
    return results;
  }

  // String operations
  set(key: string, value: string | number | Buffer): Multi {
    return this.addCommand("set", [key, value]);
  }

  get(key: string): Multi {
    return this.addCommand("get", [key]);
  }

  // Hash operations
  hSet(
    key: string,
    field: string | Record<string, any>,
    value?: string | number | Buffer
  ): Multi {
    if (typeof field === "object") {
      return this.addCommand("hSet", [key, field]);
    }
    return this.addCommand("hSet", [key, field, value]);
  }

  hGet(key: string, field: string): Multi {
    return this.addCommand("hGet", [key, field]);
  }

  hDel(key: string, ...fields: string[]): Multi {
    return this.addCommand("hDel", [key, ...fields]);
  }

  // List operations
  lPush(key: string, ...values: (string | number | Buffer)[]): Multi {
    return this.addCommand("lPush", [key, ...values]);
  }

  rPush(key: string, ...values: (string | number | Buffer)[]): Multi {
    return this.addCommand("rPush", [key, ...values]);
  }

  lPop(key: string): Multi {
    return this.addCommand("lPop", [key]);
  }

  rPop(key: string): Multi {
    return this.addCommand("rPop", [key]);
  }

  // Set operations
  sAdd(key: string, ...members: (string | number | Buffer)[]): Multi {
    return this.addCommand("sAdd", [key, ...members]);
  }

  sRem(key: string, ...members: (string | number | Buffer)[]): Multi {
    return this.addCommand("sRem", [key, ...members]);
  }

  // Key operations
  del(key: string | string[]): Multi {
    return this.addCommand("del", [key]);
  }

  exists(key: string | string[]): Multi {
    return this.addCommand("exists", [key]);
  }

  // Expiration operations
  expire(key: string, seconds: number): Multi {
    return this.addCommand("expire", [key, seconds]);
  }

  ttl(key: string): Multi {
    return this.addCommand("ttl", [key]);
  }

  private addCommand(command: string, args: any[]): Multi {
    this.commands.push({
      command,
      args,
      resolve: () => {},
      reject: () => {},
    });
    return this;
  }
}

// Export everything
export {
  RedisClientOptions,
  RedisClientError,
  CommandOptions,
  RedisClient,
  RedisClient as default,
};

// Export createClient for compatibility with node-redis
export const createClient = RedisClient.createClient;
