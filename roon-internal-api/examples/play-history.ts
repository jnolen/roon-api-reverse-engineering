/**
 * Play-history export via VirtualHistoryQuery (READ-ONLY).
 *   npx ts-node examples/play-history.ts [limit]
 *
 * Walks the profile's play history, newest first, and prints one JSON line
 * per play: { playedAt, artist, title, album?, completionPct?, roonTrackId? }.
 *
 * Wire facts (live-probed):
 * - HistoryPlay::Time is a .NET DateTime int64 — the top two bits are the
 *   Kind (Utc = bit 62), the low 62 bits are ticks (100ns) since
 *   0001-01-01 UTC.
 * - HistoryPlay::HistoryPlayId equals the raw ticks of Time — Roon keys each
 *   play by its timestamp, so it doubles as a stable per-play identity.
 * - HistoryPlay::TrackBase is an inline TrackLink value struct carrying the
 *   STABLE TrackId (the same id family the favorite/playlist flows use).
 * - HistoryPlay::Track refs a TrackLite (Title, LengthSeconds, Album ref);
 *   the referenced AlbumLite carries Title + PerformedBy (album artist).
 * - RetainPage/ReleasePage resolve only under the DERIVED declaring type
 *   (VirtualHistoryPlayQuery::), and the Core never invokes ReleasePage's
 *   ResultCallback — send it fire-and-forget, exactly once per page (see
 *   docs on object lifetime; over-releasing corrupts refcounts).
 */
import { RoonClient } from '../src/proto/client';
import { Arg, buildArgs, inlineStruct } from '../src/proto/serializer';
import { PropertyType, RoonObject } from '../src/proto/objects';
import { BinaryWriter } from '../src/proto/writer';
import { readFlexLong } from '../src/proto/flex';

const SIG_RETAIN_PAGE =
  'Sooloos.Broker.Api.VirtualHistoryPlayQuery::RetainPage(int, Base.ResultCallback)';
const SIG_RELEASE_PAGE =
  'Sooloos.Broker.Api.VirtualHistoryPlayQuery::ReleasePage(int, Base.ResultCallback)';

const TICKS_MASK = (1n << 62n) - 1n; // strip the DateTime Kind bits
const UNIX_EPOCH_TICKS = 621355968000000000n;

/** Decode a raw .NET DateTime int64 (Kind bits tolerated) to ISO 8601 UTC. */
function dotnetTicksToIso(raw: bigint): string {
  const ticks = raw & TICKS_MASK;
  return new Date(Number((ticks - UNIX_EPOCH_TICKS) / 10000n)).toISOString();
}

/** Find a field by member-name suffix (graph keys are fully qualified). */
function field(o: RoonObject | undefined, suffix: string): unknown {
  if (!o) return undefined;
  return Object.entries(o.fields).find(([k]) => k.endsWith(suffix))?.[1];
}

function refOf(v: unknown): bigint | undefined {
  if (v && typeof v === 'object' && '$ref' in (v as object)) {
    return BigInt(String((v as { $ref: unknown }).$ref));
  }
  return undefined;
}

/** Poll until read() returns a value or the time budget runs out. */
async function pollFor<T>(ms: number, read: () => T | undefined): Promise<T | undefined> {
  const deadline = Date.now() + ms;
  for (;;) {
    const v = read();
    if (v !== undefined) return v;
    if (Date.now() >= deadline) return undefined;
    await new Promise((r) => setTimeout(r, 150));
  }
}

(async () => {
  const limit = Number(process.argv[2] || 50);
  const pageSize = 100;
  const roon = new RoonClient({
    host: process.env.ROON_HOST || 'YOUR_CORE_IP',
    serverBrokerId: Buffer.from(process.env.ROON_BROKER_ID || 'YOUR_SERVER_BROKER_ID', 'hex'),
  });
  await roon.connect();
  try {
    // Empty criteria = server defaults (date-descending, matching Roon's own
    // history view). PageSize uses the full member name — the Core silently
    // discards short DEFTYPE member names (#8).
    const criteria = inlineStruct(
      roon.remoting.defineType('Sooloos.Broker.Api.HistoryQueryCriteria', [])
    );
    const params = roon.structArg('Sooloos.Broker.Api.VirtualQueryParameters', [
      {
        name: 'int Sooloos.Broker.Api.VirtualQueryParameters::PageSize',
        propType: PropertyType.Int,
        value: new BinaryWriter().integer(pageSize).toBuffer(),
      },
    ]);
    const args = Buffer.concat([buildArgs([Arg.sooid(roon.profile())]), criteria, params]);
    const res = await roon.call(
      'Library',
      'VirtualHistoryQuery',
      [
        { type: 'Sooid', name: 'profileid' },
        { type: 'HistoryQueryCriteria', name: 'criteria' },
        { type: 'VirtualQueryParameters', name: 'queryparams' },
        { type: 'ResultCallback<VirtualHistoryPlayQuery>', name: 'cb' },
      ],
      args
    );
    if (!res.success) throw new Error(`VirtualHistoryQuery failed: ${res.status}`);
    const [queryOid] = readFlexLong(Uint8Array.from(res.payload), 0);

    // Poll the query object's ::Count instead of sleeping a fixed interval.
    const readCount = () => {
      const c = field(roon.graph.getObject(queryOid), '::Count');
      return typeof c === 'number' ? c : undefined;
    };
    const total = (await pollFor(5000, readCount)) ?? 0;
    console.error(`history: ${total} play(s) on the Core; exporting up to ${limit}`);
    if (!total) return;

    const target = Math.min(total, limit);
    const plays = new Map<string, RoonObject>();
    const harvest = () => {
      for (const o of roon.graph.findByType('HistoryPlay')) {
        const id = field(o, '::HistoryPlayId') ?? field(o, '::Time');
        if (id !== undefined && !plays.has(String(id))) plays.set(String(id), o);
      }
      return plays.size;
    };

    // A page can come up slightly short of pageSize, so the stop condition is
    // "a page added nothing", never a cumulative count. Hard cap = data size
    // plus slack.
    const maxPages = Math.ceil(total / pageSize) + 2;
    for (let page = 0; plays.size < target && page < maxPages; page++) {
      const before = plays.size;
      const rp = await roon.remoting.callMethod(queryOid, SIG_RETAIN_PAGE, buildArgs([Arg.int(page)]));
      if (!rp.success) break;
      const pageTarget = Math.min(target, before + pageSize);
      await pollFor(5000, () => (harvest() >= pageTarget ? plays.size : undefined));
      // Fire-and-forget: the Core executes ReleasePage but never invokes its
      // ResultCallback (awaiting would time out). Exactly once per page.
      roon.remoting.callMethodNoReply(queryOid, SIG_RELEASE_PAGE, buildArgs([Arg.int(page)]));
      if (plays.size === before) break; // page added nothing: end of data
    }

    // Let straggling TrackLite/AlbumLite pushes land before assembly.
    await new Promise((r) => setTimeout(r, 1000));

    let printed = 0;
    let skipped = 0;
    for (const play of plays.values()) {
      if (printed >= limit) break;
      const time = field(play, '::Time');
      if (typeof time !== 'bigint') {
        skipped++;
        continue;
      }
      const track = (() => {
        const r = refOf(field(play, '::Track'));
        return r === undefined ? undefined : roon.graph.getObject(r);
      })();
      const album = (() => {
        const r = refOf(field(track, '::Album'));
        return r === undefined ? undefined : roon.graph.getObject(r);
      })();
      const title = field(track, '::Title');
      const artist = field(album, '::PerformedBy');
      if (typeof title !== 'string' || typeof artist !== 'string') {
        // TrackLite gone from the library and never pushed: can't satisfy
        // artist+title, count as skipped.
        skipped++;
        continue;
      }
      const ev: Record<string, unknown> = { playedAt: dotnetTicksToIso(time), artist, title };
      const albumTitle = field(album, '::Title');
      if (typeof albumTitle === 'string') ev.album = albumTitle;
      const secs = field(play, '::SecondsPlayed');
      const len = field(track, '::LengthSeconds');
      if (typeof secs === 'number' && typeof len === 'number' && len > 0) {
        ev.completionPct = Math.min(100, Math.round((secs / len) * 1000) / 10);
      }
      const link = field(play, '::TrackBase');
      if (link && typeof link === 'object') {
        const tid = Object.entries(link as Record<string, unknown>).find(([k]) =>
          k.endsWith('::TrackId')
        )?.[1];
        if (tid !== undefined) ev.roonTrackId = String(tid);
      }
      console.log(JSON.stringify(ev));
      printed++;
    }
    console.error(`exported ${printed} play(s), skipped ${skipped} (unresolvable track)`);
  } finally {
    roon.close();
  }
})().catch((e) => {
  console.error('FAILED:', e.message);
  process.exit(1);
});
