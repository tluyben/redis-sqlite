import net from "net";
import { EventEmitter } from "events";
import { RedisSQLite } from "./redis-sqlite";

const CRLF = "\r\n";

enum RESPDataType {
  SimpleString = "+",
  Error = "-",
  Integer = ":",
  BulkString = "$",
  Array = "*",
}

interface ServerOptions {
  port?: number;
  host?: string;
  filename?: string;
  password?: string;
}

export class RedisServer extends EventEmitter {
  private server: net.Server;
  private client: RedisSQLite;
  private connections: Set<net.Socket> = new Set();
  private authenticatedSockets: Set<net.Socket> = new Set();

  constructor(private options: ServerOptions = {}) {
    super();
    this.server = net.createServer((socket) => this.handleConnection(socket));
    this.client = new RedisSQLite({
      filename: options.filename || ":memory:",
      password: options.password,
    });
  }

  async start(): Promise<void> {
    await this.client.connect();
    const port = this.options.port || 6379;
    const host = this.options.host || "127.0.0.1";

    return new Promise((resolve) => {
      this.server.listen(port, host, () => {
        this.emit("ready");
        resolve();
      });
    });
  }

  async stop(): Promise<void> {
    // Close all active connections
    for (const socket of this.connections) {
      socket.destroy();
    }
    this.connections.clear();

    // Close server and database
    await this.client.close();
    return new Promise((resolve) => {
      this.server.close(() => {
        this.emit("closed");
        resolve();
      });
    });
  }

  private handleConnection(socket: net.Socket): void {
    this.connections.add(socket);
    let buffer = "";

    socket.on("data", (data) => {
      buffer += data.toString();

      // Process complete RESP messages
      let message;
      while ((message = this.extractMessage(buffer)) !== null) {
        buffer = buffer.slice(message.length);
        this.processCommand(socket, this.parseRESP(message));
      }
    });

    socket.on("error", (error) => {
      this.emit("error", error);
    });

    socket.on("close", () => {
      this.connections.delete(socket);
      this.authenticatedSockets.delete(socket);
    });
  }

  private extractMessage(buffer: string): string | null {
    if (!buffer.includes(CRLF)) return null;

    // Handle different RESP types
    const firstChar = buffer[0];
    switch (firstChar) {
      case RESPDataType.Array: {
        return this.extractArray(buffer);
      }
      case RESPDataType.BulkString: {
        return this.extractBulkString(buffer);
      }
      case RESPDataType.SimpleString:
      case RESPDataType.Error:
      case RESPDataType.Integer: {
        const endIndex = buffer.indexOf(CRLF);
        return buffer.slice(0, endIndex + 2);
      }
      default:
        return null;
    }
  }

  private extractArray(buffer: string): string | null {
    const firstLine = buffer.split(CRLF)[0];
    const arrayLength = parseInt(firstLine.slice(1));
    if (isNaN(arrayLength)) return null;

    let currentPos = firstLine.length + 2; // Skip first line and CRLF
    let elementsFound = 0;

    while (elementsFound < arrayLength) {
      if (currentPos >= buffer.length) return null;

      const elementType = buffer[currentPos];
      switch (elementType) {
        case RESPDataType.BulkString: {
          const remainingBuffer = buffer.slice(currentPos);
          const bulkString = this.extractBulkString(remainingBuffer);
          if (!bulkString) return null;
          currentPos += bulkString.length;
          elementsFound++;
          break;
        }
        default:
          // Handle other types if needed
          return null;
      }
    }

    return buffer.slice(0, currentPos);
  }

  private extractBulkString(buffer: string): string | null {
    const firstLine = buffer.split(CRLF)[0];
    const strLength = parseInt(firstLine.slice(1));
    if (isNaN(strLength)) return null;

    if (strLength === -1) return `${firstLine}${CRLF}`;

    const expectedLength = firstLine.length + 2 + strLength + 2;
    if (buffer.length < expectedLength) return null;

    return buffer.slice(0, expectedLength);
  }

  private parseRESP(message: string): any {
    const type = message[0];
    const content = message.slice(1, -2); // Remove type char and CRLF

    switch (type) {
      case RESPDataType.Array: {
        const arrayLength = parseInt(content);
        if (arrayLength === -1) return null;

        const elements = [];
        let currentPos = message.indexOf(CRLF) + 2;

        for (let i = 0; i < arrayLength; i++) {
          const elementMessage = this.extractMessage(message.slice(currentPos));
          if (!elementMessage) break;

          elements.push(this.parseRESP(elementMessage));
          currentPos += elementMessage.length;
        }

        return elements;
      }
      case RESPDataType.BulkString: {
        const strLength = parseInt(content);
        if (strLength === -1) return null;

        const startPos = message.indexOf(CRLF) + 2;
        return message.slice(startPos, startPos + strLength);
      }
      case RESPDataType.SimpleString:
        return content;
      case RESPDataType.Error:
        return new Error(content);
      case RESPDataType.Integer:
        return parseInt(content);
      default:
        return null;
    }
  }

  private async processCommand(
    socket: net.Socket,
    parsed: any[]
  ): Promise<void> {
    if (!Array.isArray(parsed) || parsed.length === 0) {
      this.sendError(socket, "Invalid command format");
      return;
    }

    const [command, ...args] = parsed;
    const cmd = command.toString().toLowerCase();

    // Handle authentication
    if (cmd === "auth") {
      try {
        await this.client.auth(args[0]);
        this.authenticatedSockets.add(socket);
        this.sendResponse(socket, "OK");
      } catch (error) {
        if (error instanceof Error) {
          this.sendError(socket, error.message);
        } else {
          this.sendError(socket, "Unknown error occurred");
        }
      }
      return;
    }

    // Check authentication if password is set
    if (
      this.options.password &&
      !this.authenticatedSockets.has(socket) &&
      cmd !== "quit"
    ) {
      this.sendError(socket, "NOAUTH Authentication required.");
      return;
    }

    try {
      // @ts-ignore - we know these methods exist
      const result = await this.client[cmd](...args);
      this.sendResponse(socket, result);
    } catch (error) {
      if (error instanceof Error) {
        this.sendError(socket, error.message);
      } else {
        this.sendError(socket, "Unknown error occurred");
      }
    }
  }

  private sendResponse(socket: net.Socket, data: any): void {
    if (data === null) {
      socket.write(`$-1${CRLF}`);
    } else if (typeof data === "number") {
      socket.write(`:${data}${CRLF}`);
    } else if (typeof data === "string") {
      if (data === "OK") {
        socket.write(`+${data}${CRLF}`);
      } else {
        socket.write(`$${data.length}${CRLF}${data}${CRLF}`);
      }
    } else if (Array.isArray(data)) {
      socket.write(`*${data.length}${CRLF}`);
      for (const item of data) {
        this.sendResponse(socket, item);
      }
    } else if (data instanceof Error) {
      socket.write(`-${data.message}${CRLF}`);
    } else if (typeof data === "object") {
      const entries = Object.entries(data);
      socket.write(`*${entries.length * 2}${CRLF}`);
      for (const [key, value] of entries) {
        this.sendResponse(socket, key);
        this.sendResponse(socket, value);
      }
    }
  }

  private sendError(socket: net.Socket, message: string): void {
    socket.write(`-ERR ${message}${CRLF}`);
  }
}

// Export default
export default RedisServer;
