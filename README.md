# 🚀 SQLite-backed Redis Implementation

A lightweight Redis-compatible implementation backed by SQLite, perfect for development, testing, and situations where you need Redis semantics without the Redis server.

## ✨ Features

### 🔄 Multiple Interface Options

- 🔌 Redis Wire Protocol Server
- 📦 IORedis-compatible client
- 🟥 Node-Redis compatible client
- 🛠️ Base SQLite implementation

### 💾 Storage Options

- 🧠 In-memory database
- 📁 File-based persistence
- 🔄 Automatic cleanup of expired keys

### 🔐 Authentication Support

- 🔑 Password protection
- 🚫 Connection-level auth tracking
- ⚡ Compatible with standard Redis auth

### 📝 Supported Redis Commands

- ✅ Strings: SET, GET
- 📋 Lists: LPUSH, RPUSH, LPOP, RPOP, RPOPLPUSH, BRPOPLPUSH
- 📑 Hashes: HSET, HGET, HDEL, HMSET, HMGET
- 🎯 Sets: SADD, SREM, SISMEMBER, SMEMBERS
- ⏰ Expiration: EXPIRE, TTL
- 🔐 Auth: AUTH
- 🎬 Transactions: MULTI/EXEC

## 🚀 Getting Started

### Installation

```bash
npm install redis-sqlite
# or
yarn add redis-sqlite
```

### 💻 Usage Examples

#### 🔌 As a Redis Server

```typescript
import { RedisServer } from "redis-sqlite/server";

const server = new RedisServer({
  port: 6379,
  password: "optional-password",
});

await server.start();
```

#### 📦 With IORedis

```typescript
import { Redis } from "redis-sqlite/ioredis";

const redis = new Redis({
  filename: ":memory:", // or path to file
  password: "optional-password",
});

await redis.set("key", "value");
const value = await redis.get("key");
```

#### 🟥 With Node-Redis

```typescript
import { createClient } from "redis-sqlite/node-redis";

const client = await createClient({
  filename: ":memory:", // or path to file
  password: "optional-password",
});

await client.set("key", "value");
const value = await client.get("key");
```

## ⚠️ Limitations

### 🚫 Not Implemented

- Pub/Sub functionality
- Lua scripting (EVAL/EVALSHA)
- Cluster support
- Redis 6+ ACL features
- Some advanced Redis commands

### ⚡ Performance Considerations

- Not designed for high-throughput production use (but suitable for most applications)
- Single-threaded SQLite backend
- Limited by SQLite's concurrency model
- Best suited for development/testing

### 💾 Storage Limits

- Subject to SQLite database size limits
- Memory usage scales with database size
- No built-in database size management

## 🤝 Contributing

Contributions are welcome! Please feel free to submit pull requests.

## 📝 License

MIT License - see LICENSE file for details.

## 🙏 Acknowledgments

- SQLite team for the amazing database
- Redis team for the protocol specification
