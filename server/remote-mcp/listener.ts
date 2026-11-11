import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { chmodSync, mkdirSync, rmSync } from "node:fs";
import net from "node:net";
import path from "node:path";
import { PassThrough } from "node:stream";

import { createPinnedClient, type PinnedDeps } from "./pinned-client.ts";
import { parsePreamble, PREAMBLE_MAX_BYTES } from "./preamble.ts";
import { CORRAL_HOME } from "../../config.ts";
import { createIdentity } from "../../mcp/identity.ts";
import { ORIENTATION } from "../../mcp/orientation.ts";
import { registerSelfTool } from "../../mcp/tools/self.ts";
import { registerSessionTools } from "../../mcp/tools/session.ts";
import { registerUpdateTool } from "../../mcp/tools/task-update.ts";
import { registerTaskTools } from "../../mcp/tools/task.ts";

// 0700: the local half of the per-user rule sshd enforces on the remote end of the forward.
export const MCP_SOCKET_DIR = path.join(CORRAL_HOME, "mcp");

export function localSocketPath(envId: string): string {
  return path.join(MCP_SOCKET_DIR, `${envId}.sock`);
}

export const PREAMBLE_TIMEOUT_MS = 15_000;
// Far above honest load (one connection per session); it bounds fd use against a looping peer.
export const MAX_CONNECTIONS = 64;

// Injectable so tests can fire the preamble deadline deterministically instead of waiting 15s.
export interface ListenerClock {
  // Returns a cancel function; the real clock also unrefs so a pending preamble never holds the process.
  setTimeout(callback: () => void, ms: number): () => void;
}

export interface ListenerOptions {
  readonly clock?: ListenerClock;
  readonly preambleTimeoutMs?: number;
  readonly preambleMaxBytes?: number;
  readonly maxConnections?: number;
}

interface ResolvedListenerOptions {
  readonly clock: ListenerClock;
  readonly preambleTimeoutMs: number;
  readonly preambleMaxBytes: number;
  readonly maxConnections: number;
}

const systemClock: ListenerClock = {
  setTimeout: (callback, ms) => {
    const timer = setTimeout(callback, ms);
    timer.unref();
    return () => { clearTimeout(timer); };
  },
};

// One MCP server per connection, its environment fixed by which listener accepted it (ADR 0009).
function serveConnection(conn: net.Socket, deps: PinnedDeps, baseUrl: string, opts: ResolvedListenerOptions): void {
  let buf = Buffer.alloc(0);
  let started = false;

  const fail = (reason: string): void => {
    console.error(`[remote-mcp] ${deps.env.id}: dropping connection — ${reason}`);
    conn.destroy();
  };

  let sawBytes = false;
  const cancelTimer = opts.clock.setTimeout(() => {
    if (started) return;
    // Silent when nothing was said at all: corral's own tunnel probe connects and hangs up each tick.
    if (sawBytes) fail("no preamble within the deadline");
    else conn.destroy();
  }, opts.preambleTimeoutMs);

  const start = (line: string, rest: Buffer): void => {
    const parsed = parsePreamble(line);
    if (!parsed.ok) {
      fail(parsed.reason);
      return;
    }
    // Bytes sharing the preamble's chunk are pushed ahead of the pipe rather than lost.
    const through = new PassThrough();
    if (rest.length > 0) through.write(rest);
    conn.pipe(through);

    const client = createPinnedClient(baseUrl, deps);
    const identity = createIdentity(client, {
      paneId: parsed.preamble.paneId,
      cwd: parsed.preamble.cwd,
      socket: null,
    });
    // The one record that a remote session used these tools; ADR 0009 allows same-user impersonation.
    console.warn(`[remote-mcp] ${deps.env.id}: session opened for pane ${parsed.preamble.paneId}`);
    const server = new McpServer({ name: "corral", version: "0.1.0" }, { instructions: ORIENTATION });
    registerSelfTool(server, identity);
    registerTaskTools(server, { client, identity });
    registerUpdateTool(server, { client, identity });
    registerSessionTools(server, { client, identity, envScope: deps.env.id });

    const transport = new StdioServerTransport(through, conn);
    // The SDK's transport never closes itself, so on a socket the peer's disconnect must release it.
    let released = false;
    const release = (): void => {
      if (released) return;
      released = true;
      through.end();
      void server.close().catch(() => undefined);
      conn.destroy();
    };
    conn.on("close", release);
    conn.on("end", release);
    conn.on("error", release);
    transport.onclose = release;
    transport.onerror = release;

    server.connect(transport).catch((err: unknown) => {
      console.error(`[remote-mcp] ${deps.env.id}: session failed — ${err instanceof Error ? err.message : String(err)}`);
      release();
    });
    conn.resume();
  };

  const onData = (chunk: Buffer): void => {
    sawBytes = true;
    buf = Buffer.concat([buf, chunk]);
    const nl = buf.indexOf(0x0a);
    if (nl === -1) {
      if (buf.length > opts.preambleMaxBytes) fail("preamble exceeded its size limit with no newline");
      return;
    }
    // Both synchronous and inside the handler, so no chunk is read twice or dropped in between.
    conn.off("data", onData);
    conn.pause();
    started = true;
    cancelTimer();
    start(buf.subarray(0, nl).toString("utf8"), buf.subarray(nl + 1));
  };

  conn.on("data", onData);
  conn.on("error", (err) => {
    console.error(`[remote-mcp] ${deps.env.id}: connection error — ${err.message}`);
    // After the preamble `release` handles this; this covers the pre-preamble half.
    if (!started) conn.destroy();
  });
}

export interface RemoteMcpListener {
  readonly socketPath: string;
  close(): Promise<void>;
}

// Unlinks first: a corral that was killed leaves its socket file behind and the bind would EADDRINUSE.
export async function startEnvListener(
  deps: PinnedDeps,
  baseUrl: string,
  options: ListenerOptions = {},
): Promise<RemoteMcpListener> {
  const opts: ResolvedListenerOptions = {
    clock: options.clock ?? systemClock,
    preambleTimeoutMs: options.preambleTimeoutMs ?? PREAMBLE_TIMEOUT_MS,
    preambleMaxBytes: options.preambleMaxBytes ?? PREAMBLE_MAX_BYTES,
    maxConnections: options.maxConnections ?? MAX_CONNECTIONS,
  };

  mkdirSync(MCP_SOCKET_DIR, { recursive: true, mode: 0o700 });
  chmodSync(MCP_SOCKET_DIR, 0o700); // mkdir's mode goes through the umask

  const socketPath = localSocketPath(deps.env.id);
  rmSync(socketPath, { force: true });

  const server = net.createServer();
  server.maxConnections = opts.maxConnections;
  // close() only stops accepting; these connections are long-lived, so it would never settle.
  const live = new Set<net.Socket>();
  server.on("connection", (conn) => {
    live.add(conn);
    conn.on("close", () => live.delete(conn));
    serveConnection(conn, deps, baseUrl, opts);
  });
  // Past maxConnections node closes the socket itself and emits "drop" instead of "connection",
  // so the peer gets an explicit close rather than hanging — and nothing enters the live set.
  server.on("drop", () => {
    console.error(
      `[remote-mcp] ${deps.env.id}: rejecting connection — connection limit of ${String(opts.maxConnections)} reached`,
    );
  });
  server.on("error", (err) => {
    console.error(`[remote-mcp] ${deps.env.id}: listener error — ${err.message}`);
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, () => {
      server.off("error", reject);
      resolve();
    });
  });
  chmodSync(socketPath, 0o600);

  return {
    socketPath,
    close: () => new Promise<void>((resolve) => {
      server.close(() => { resolve(); });
      for (const conn of live) conn.destroy();
      live.clear();
    }),
  };
}
