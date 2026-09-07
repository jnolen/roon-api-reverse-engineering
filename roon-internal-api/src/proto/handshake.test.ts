import * as net from 'net';
import { RoonConnection } from './connection';

// Regression tests for #10: the handshake must tolerate TCP delivering its
// fixed-length reply records in arbitrary fragments (and coalesced), preserve
// bytes that trail the final record for the remoting layer, and reject when
// the peer closes before the handshake completes.

const MAGIC = Buffer.from('ROON');
const ACK = Buffer.concat([MAGIC, Buffer.from([0x01, 0x80])]); // 6 bytes
const SESSION = Buffer.concat([MAGIC, Buffer.from([0x01, 0x82]), Buffer.alloc(16, 0xab)]); // 22 bytes
const RESPONSE = Buffer.from('deadbeef0102030405', 'hex'); // stand-in ConnectResponse bytes

const GAP_MS = 15;

function splitAt(buf: Buffer, i: number): Buffer[] {
  return [buf.subarray(0, i), buf.subarray(i)];
}

function everyByte(buf: Buffer): Buffer[] {
  return [...buf].map((b) => Buffer.from([b]));
}

async function writeFragments(socket: net.Socket, fragments: Buffer[]): Promise<void> {
  for (const f of fragments) {
    if (socket.destroyed) return;
    socket.write(f);
    await new Promise((r) => setTimeout(r, GAP_MS));
  }
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

interface CoreScript {
  ackFragments: Buffer[];
  /** End the connection after the ack fragments instead of continuing. */
  endAfterAck?: boolean;
  sessionFragments?: Buffer[];
  responseFragments?: Buffer[];
  /**
   * The ack fragments already carry the session record too (coalescing test);
   * do not send it again when the client's 0102 arrives.
   */
  sessionSentWithAck?: boolean;
}

/**
 * Fake Core that walks the real handshake but delivers each reply in the
 * scripted fragments. Client messages are counted cumulatively (hello = 38
 * bytes, then 0102 = 6, then the ConnectRequest), which is safe because the
 * client only sends each message after the previous reply completes.
 */
function startCore(port: number, script: CoreScript): Promise<{ close: () => Promise<void> }> {
  const sockets: net.Socket[] = [];
  const server = net.createServer((socket) => {
    sockets.push(socket);
    socket.setNoDelay(true);
    let received = 0;
    let stage = 0;
    socket.on('data', (d: Buffer) => {
      received += d.length;
      if (stage === 0 && received >= 38) {
        stage = 1;
        received -= 38;
        void writeFragments(socket, script.ackFragments).then(() => {
          if (script.endAfterAck) socket.end();
        });
        return;
      }
      if (stage === 1 && received >= 6) {
        stage = 2;
        received -= 6;
        if (!script.sessionSentWithAck) void writeFragments(socket, script.sessionFragments ?? [SESSION]);
        if (received > 0) {
          // The ConnectRequest coalesced with the 0102 (happens when the
          // session record was pre-sent): respond now, not on a next event.
          stage = 3;
          void writeFragments(socket, script.responseFragments ?? [RESPONSE]);
        }
        return;
      }
      if (stage === 2) {
        stage = 3;
        void writeFragments(socket, script.responseFragments ?? [RESPONSE]);
      }
    });
    socket.on('error', () => {});
  });
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', () => {
      resolve({
        close: () =>
          new Promise<void>((res) => {
            for (const s of sockets) s.destroy();
            server.close(() => res());
          }),
      });
    });
  });
}

/** Connect and return everything the remoting layer received, once complete. */
async function connectAndCollect(port: number, expectedLen: number): Promise<Buffer> {
  const conn = new RoonConnection({ host: '127.0.0.1', port, serverBrokerId: Buffer.alloc(16) });
  const chunks: Buffer[] = [];
  conn.onData((c) => chunks.push(c));
  try {
    await conn.connect();
    const deadline = Date.now() + 2000;
    while (Buffer.concat(chunks).length < expectedLen && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 10));
    }
    return Buffer.concat(chunks);
  } finally {
    conn.close();
  }
}

describe('handshake record buffering (#10)', () => {
  test('unsplit control: whole records, one write each', async () => {
    const port = await grabEphemeralPort();
    const core = await startCore(port, { ackFragments: [ACK] });
    try {
      const got = await connectAndCollect(port, RESPONSE.length);
      expect(got.equals(RESPONSE)).toBe(true);
    } finally {
      await core.close();
    }
  });

  test.each([1, 2, 3, 4, 5])('ack split at byte %i (the issue repro is 3)', async (i) => {
    const port = await grabEphemeralPort();
    const core = await startCore(port, { ackFragments: splitAt(ACK, i) });
    try {
      const got = await connectAndCollect(port, RESPONSE.length);
      expect(got.equals(RESPONSE)).toBe(true);
    } finally {
      await core.close();
    }
  });

  test.each(Array.from({ length: 21 }, (_, k) => k + 1))(
    'session record split at byte %i',
    async (i) => {
      const port = await grabEphemeralPort();
      const core = await startCore(port, {
        ackFragments: [ACK],
        sessionFragments: splitAt(SESSION, i),
      });
      try {
        const got = await connectAndCollect(port, RESPONSE.length);
        expect(got.equals(RESPONSE)).toBe(true);
      } finally {
        await core.close();
      }
    }
  );

  test('byte-at-a-time delivery of the entire handshake', async () => {
    const port = await grabEphemeralPort();
    const core = await startCore(port, {
      ackFragments: everyByte(ACK),
      sessionFragments: everyByte(SESSION),
      responseFragments: splitAt(RESPONSE, 4),
    });
    try {
      const got = await connectAndCollect(port, RESPONSE.length);
      expect(got.equals(RESPONSE)).toBe(true);
    } finally {
      await core.close();
    }
  }, 15000);

  test('coalesced ack + session record in a single write', async () => {
    const port = await grabEphemeralPort();
    // Both records arrive in one read; the parser must consume them in order
    // (the client's own 0102 goes out between consuming the two).
    const core = await startCore(port, {
      ackFragments: [Buffer.concat([ACK, SESSION])],
      sessionSentWithAck: true,
    });
    try {
      const got = await connectAndCollect(port, RESPONSE.length);
      expect(got.equals(RESPONSE)).toBe(true);
    } finally {
      await core.close();
    }
  });

  test('fragmented ConnectResponse reaches the remoting layer intact', async () => {
    const port = await grabEphemeralPort();
    const core = await startCore(port, {
      ackFragments: [ACK],
      responseFragments: everyByte(RESPONSE),
    });
    try {
      const got = await connectAndCollect(port, RESPONSE.length);
      expect(got.equals(RESPONSE)).toBe(true);
    } finally {
      await core.close();
    }
  });

  test('peer closing mid-handshake rejects instead of hanging', async () => {
    const port = await grabEphemeralPort();
    // Send only the first 3 bytes of the ack, then FIN.
    const core = await startCore(port, {
      ackFragments: [ACK.subarray(0, 3)],
      endAfterAck: true,
    });
    const conn = new RoonConnection({ host: '127.0.0.1', port, serverBrokerId: Buffer.alloc(16) });
    try {
      await expect(conn.connect()).rejects.toThrow(/closed during handshake/);
    } finally {
      conn.close();
      await core.close();
    }
  });

  test('a complete record with an unexpected reply code fails the handshake', async () => {
    const port = await grabEphemeralPort();
    const bogus = Buffer.concat([MAGIC, Buffer.from([0x01, 0x99])]);
    const core = await startCore(port, { ackFragments: [bogus] });
    const conn = new RoonConnection({ host: '127.0.0.1', port, serverBrokerId: Buffer.alloc(16) });
    try {
      await expect(conn.connect()).rejects.toThrow(/unexpected handshake reply 0x99/);
    } finally {
      conn.close();
      await core.close();
    }
  });
});
