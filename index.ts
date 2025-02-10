// Base implementation
export * from "./redis-sqlite";

// Redis Server
export * from "./redis-server";

// IORedis implementation
import * as IORedis from "./ioredis-sqlite";
export { IORedis };
// Convenient re-exports for IORedis
export * from "./ioredis-sqlite";

// Node-Redis implementation
import * as NodeRedis from "./node-redis-sqlite";
export { NodeRedis };
// Convenient re-exports for Node-Redis
export * from "./node-redis-sqlite";

// Make the implementations available under the same names as the original packages
import { Redis as IORedisImpl } from "./ioredis-sqlite";
import { createClient as createNodeRedisClient } from "./node-redis-sqlite";

// Export under 'ioredis' name
export const ioredis = {
  Redis: IORedisImpl,
  default: IORedisImpl,
};

// Export under 'node-redis' name
export const redis = {
  createClient: createNodeRedisClient,
};

// Also export individually for more flexible importing
export const Redis = IORedisImpl;
export const createClient = createNodeRedisClient;
