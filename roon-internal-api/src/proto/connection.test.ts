import * as net from 'net';
import { RoonConnection } from './connection';
import { RemotingClient } from './remoting';
import { encodeResponse } from './frame';

const MAGIC = Buffer.from('ROON');

/**
 * Minimal fake Core: completes the ROON handshake (hello -> 0180, 0102 ->
 * 0182, ConnectRequest -> one benign remoting frame) and then goes silent, so
 * a remoting request can be left in flight when a test drops the connection.
 */
function startFakeCore(
  port: number
): Promise<{ sockets: net.Socket[]; close: () => Promise<void> }> {
  const sockets: net.Socket[] = [];
  const server = net.createServer((socket) => {
    sockets.push(socket);
    let step = 0;
    socket.on('data', (data: Buffer) => {
      if (step === 0 && data.subarray(0, 4).equals(MAGIC)) {
        step = 1;
        socket.write(Buffer.concat([MAGIC, Buffer.from([0x01, 0x80])]));
        return;
      }
      if (step === 1 && data.subarray(0, 4).equals(MAGIC)) {
        step = 2;
        socket.write(Buffer.concat([MAGIC, Buffer.from([0x01, 0x82]), Buffer.alloc(4)]));
        return;
      }
      if (step === 2) {
        // The ConnectRequest arrived. Any non-ROON bytes complete the client
        // handshake; a well-formed response frame addressed to an unknown
        // request id keeps the client's FrameParser in sync afterwards.
        step = 3;
        socket.write(encodeResponse(0x7ff0, Buffer.alloc(0)));
        return;
      }
      // step 3+: stay silent — requests are left in flight deliberately.
    });
    socket.on('error', () => {});
  });
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', () => {
      resolve({
        sockets,
        close: () =>
          new Promise<void>((res) => {
            for (const s of sockets) s.destroy();
            server.close(() => res());
          }),
      });
    });
  });
}

/** Reserve an ephemeral port by binding to 0 and releasing it. */
function grabEphemeralPort(): Promise<number> {
  return new Promise((resolve) => {
    const s = net.createServer();
    s.listen(0, '127.0.0.1', () => {
      const port = (s.address() as net.AddressInfo).port;
      s.close(() => resolve(port));
    });
  });
}

describe('RoonConnection close notification across socket attempts', () => {
  // Regression for the retry case: a connect() that fails before
  // establishment must not consume the once-only close notification, or a
  // successful retry's later disconnect is silent and in-flight requests sit
  // out their timeouts instead of failing promptly.
  test(
    'failed first attempt -> successful retry -> peer disconnect still fails in-flight requests',
    async () => {
      const port = await grabEphemeralPort();
      const conn = new RoonConnection({
        host: '127.0.0.1',
        port,
        serverBrokerId: Buffer.alloc(16),
      });
      // Short request timeout so a regression rejects with the timeout
      // message (and fails the assertion) instead of stalling the test.
      const remoting = new RemotingClient(conn, 3000);
      // Same wiring RoonClient uses.
      conn.onclosed = () => remoting.failPending('broker connection closed');

      // Attempt 1: nothing is listening yet, so this fails before
      // establishment and its socket emits 'close'.
      await expect(conn.connect()).rejects.toThrow();
      // Let that socket's 'close' event land — this is what used to consume
      // the once-only notification.
      await new Promise((r) => setTimeout(r, 50));

      // Attempt 2 on the same instance succeeds.
      const core = await startFakeCore(port);
      try {
        await conn.connect();
        const inFlight = remoting.getService(new Uint8Array(16));
        await new Promise((r) => setTimeout(r, 50)); // GETSVC is now in flight
        for (const s of core.sockets) s.destroy(); // peer disconnect
        // The close reason — not the request timeout — must reject it.
        await expect(inFlight).rejects.toThrow(/broker connection closed/);
      } finally {
        await core.close();
      }
    },
    10000
  );

  test('a post-established connect() on a reused instance rejects fast instead of hanging', async () => {
    const port = await grabEphemeralPort();
    const core = await startFakeCore(port);
    const conn = new RoonConnection({
      host: '127.0.0.1',
      port,
      serverBrokerId: Buffer.alloc(16),
    });
    try {
      await conn.connect();
      // Without the guard this fed the second handshake to the frame parser
      // and stalled until the 20s socket timeout.
      await expect(conn.connect()).rejects.toThrow(/cannot reconnect/);
      // The rejected attempt must leave the live session untouched.
      expect(() => conn.send(Buffer.from([0x00]))).not.toThrow();
    } finally {
      conn.close();
      await core.close();
    }
  });
});
