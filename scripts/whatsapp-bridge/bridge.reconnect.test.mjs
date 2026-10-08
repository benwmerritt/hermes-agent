/**
 * Unit tests for the reconnect scheduling and version resolution guards.
 *
 * Regression tests for the reconnect-wedge trap: startSocket() awaits
 * network I/O (fetchLatestBaileysVersion has no AbortSignal) before it
 * creates a socket, and the close handler used to re-enter it via a bare
 * `setTimeout(startSocket, ...)`. A rejection was unhandled and a stalled
 * fetch left the bridge permanently disconnected while its HTTP server
 * kept answering 503 — observed in the field as a bridge that logged
 * "Reconnecting in 3s..." once and then went silent for 27+ hours.
 *
 * These tests avoid importing bridge.js because that file starts an HTTP
 * server and Baileys socket at module load. Keep the helper module pure.
 */

import { strict as assert } from 'node:assert';

import {
  createReconnectBackoff,
  createReconnectScheduler,
  createVersionResolver,
} from './bridge_helpers.js';

const tick = () => new Promise(resolve => setImmediate(resolve));

// -- createReconnectScheduler ---------------------------------------------

// A rejecting start function is caught and rescheduled at the retry delay;
// a subsequent success stops the retry chain.
{
  const timers = [];
  const logs = [];
  let attempts = 0;
  const startFn = async () => {
    attempts += 1;
    if (attempts === 1) throw new Error('boom');
  };

  const schedule = createReconnectScheduler(startFn, {
    retryDelayMs: 5000,
    log: line => logs.push(line),
    setTimeoutFn: (fn, ms) => timers.push({ fn, ms }),
  });

  schedule(3000);
  assert.equal(timers.length, 1);
  assert.equal(timers[0].ms, 3000);

  timers[0].fn();
  await tick();
  await tick();

  assert.equal(attempts, 1);
  assert.equal(logs.length, 1);
  assert.match(logs[0], /boom/);
  assert.equal(timers.length, 2, 'rejection must schedule a retry');
  assert.equal(timers[1].ms, 5000);

  timers[1].fn();
  await tick();
  await tick();

  assert.equal(attempts, 2);
  assert.equal(timers.length, 2, 'success must not schedule another attempt');
  assert.equal(logs.length, 1);
}

// A synchronous throw from the start function is contained the same way as
// an async rejection.
{
  const timers = [];
  const logs = [];
  const schedule = createReconnectScheduler(
    () => { throw new Error('sync boom'); },
    {
      retryDelayMs: 1000,
      log: line => logs.push(line),
      setTimeoutFn: (fn, ms) => timers.push({ fn, ms }),
    },
  );

  schedule(0);
  timers[0].fn();
  await tick();
  await tick();

  assert.equal(logs.length, 1);
  assert.match(logs[0], /sync boom/);
  assert.equal(timers.length, 2);
}

// -- createVersionResolver ------------------------------------------------

// A successful fetch returns and caches the version.
{
  const resolveVersion = createVersionResolver(
    async () => ({ version: [2, 3000, 99] }),
    { log: () => {} },
  );
  assert.deepEqual(await resolveVersion(), [2, 3000, 99]);
}

// A fetch that never settles resolves within the timeout bound instead of
// pending forever; before any success there is no cache, so the resolver
// yields null (callers fall back to the Baileys default).
{
  const logs = [];
  const resolveVersion = createVersionResolver(
    () => new Promise(() => {}),
    { timeoutMs: 20, log: line => logs.push(line) },
  );
  assert.equal(await resolveVersion(), null);
  assert.equal(logs.length, 1);
}

// After one success, later failures fall back to the cached version.
{
  const logs = [];
  let calls = 0;
  const resolveVersion = createVersionResolver(
    async () => {
      calls += 1;
      if (calls === 1) return { version: [2, 3000, 42] };
      throw new Error('network down');
    },
    { timeoutMs: 20, log: line => logs.push(line) },
  );
  assert.deepEqual(await resolveVersion(), [2, 3000, 42]);
  assert.deepEqual(await resolveVersion(), [2, 3000, 42]);
  assert.equal(logs.length, 1);
  assert.match(logs[0], /network down/);
}

// -- createReconnectBackoff -----------------------------------------------

// The close handler's two steps for a close it backs off: record it, then
// take the next rung of the ladder.
const backedOffClose = (backoff) => {
  backoff.noteClose();
  return backoff.nextDelayMs();
};

// Consecutive closes double the wait from the base up to the cap; with the
// jitter source pinned to its midpoint the delays are exact.
{
  const backoff = createReconnectBackoff({ random: () => 0.5, now: () => 0 });
  const delays = Array.from({ length: 9 }, () => backedOffClose(backoff));
  assert.deepEqual(delays, [3000, 6000, 12000, 24000, 48000, 96000, 192000, 300000, 300000]);
}

// Jitter spreads each delay ±25% around its step and never exceeds the cap.
{
  const low = createReconnectBackoff({ random: () => 0, now: () => 0 });
  const high = createReconnectBackoff({ random: () => 1, now: () => 0 });
  assert.equal(backedOffClose(low), 2250);
  assert.equal(backedOffClose(high), 3750);
  for (let i = 0; i < 10; i += 1) backedOffClose(high);
  assert.equal(backedOffClose(high), 300000, 'jitter must not push past the cap');
}

// A connection that stays open for the stable window counts as recovered:
// the next close starts over at the base delay.
{
  let clock = 0;
  const backoff = createReconnectBackoff({ random: () => 0.5, now: () => clock, stableMs: 60000 });
  backedOffClose(backoff);
  backedOffClose(backoff);
  assert.equal(backedOffClose(backoff), 12000);
  backoff.noteOpen();
  clock += 60000;
  assert.equal(backedOffClose(backoff), 3000, 'a stable connection resets the backoff');
}

// A connection that opens but drops again inside the window is still part
// of the same storm, and a close that never reached open resets nothing.
{
  let clock = 0;
  const backoff = createReconnectBackoff({ random: () => 0.5, now: () => clock, stableMs: 60000 });
  backedOffClose(backoff);
  backoff.noteOpen();
  clock += 59999;
  assert.equal(backedOffClose(backoff), 6000, 'a short-lived open must not reset');
  clock += 120000;
  assert.equal(backedOffClose(backoff), 12000, 'elapsed time without an open must not reset');
}

// A 515 close reconnects at once but is still a close: it records the stable
// open that preceded it and clears the open timestamp. Skipping that left the
// stable open on the books for the next brief open to overwrite, so a storm
// that had reached the cap, recovered, restarted on 515 and then dropped once
// more waited the full cap (300 s) instead of starting over at the base.
{
  let clock = 0;
  const backoff = createReconnectBackoff({ random: () => 0.5, now: () => clock, stableMs: 60000 });
  for (let i = 0; i < 8; i += 1) backedOffClose(backoff);
  assert.equal(backedOffClose(backoff), 300000, 'precondition: the ladder is at the cap');
  backoff.noteOpen();
  clock += 60000;
  backoff.noteClose(); // 515: no rung taken, the 1 s delay is the caller's
  backoff.noteOpen();
  clock += 1000;
  assert.equal(backedOffClose(backoff), 3000, 'the stable open before the 515 close ends the storm');
}

// The 515 close itself takes no rung: an ordinary close right after it
// continues the ladder where it was.
{
  const backoff = createReconnectBackoff({ random: () => 0.5, now: () => 0 });
  backedOffClose(backoff);
  backoff.noteClose();
  assert.equal(backedOffClose(backoff), 6000, 'a 515 close must not advance the ladder');
}

console.log('bridge.reconnect.test.mjs: all assertions passed');
