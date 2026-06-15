/**
 * POS-to-Folio Bridge Service
 * Express server that bridges Goodtill/SumUp POS "Guest Folio" payments
 * to MEWS room folios via the Connector API.
 *
 * Also syncs checked-in MEWS rooms as Goodtill customer profiles so
 * waiters can only select occupied rooms on the POS iPad.
 */

require('dotenv').config();
const crypto = require('node:crypto');
const express = require('express');
const { init, startPolling, stopPolling, pollOnce, getRoomMap, getResourceToRoom, handleVoidedSale, handleCompletedSale, getDeadLetterEntries, getStoreStats } = require('./bridge');
const { handleReservationUpdate, fullSync } = require('./roster');
const { post: mewsPost } = require('./mews');
const { ping: goodtillPing } = require('./goodtill');

const app = express();
app.use(express.json());

const PORT = parseInt(process.env.PORT || '3050', 10);
const WEBHOOK_SECRET = process.env.MEWS_WEBHOOK_SECRET || '';
const GOODTILL_WEBHOOK_TOKEN = process.env.GOODTILL_WEBHOOK_TOKEN || '';

// ─── Health check ─────────────────────────────────────────────────────

app.get('/health', (_req, res) => {
  // `store` lets ops confirm persistence: dataDir should be the mounted volume
  // path and processedSales should survive redeploys (a reset to 0 = no volume).
  res.json({ status: 'ok', uptime: process.uptime(), store: getStoreStats() });
});

app.get('/health/mews', async (_req, res) => {
  try {
    const data = await mewsPost('configuration/get');
    res.json({ status: 'ok', mews: 'ok', enterprise: data?.Enterprise?.Name || null });
  } catch (err) {
    res.status(503).json({ status: 'error', mews: 'error', message: err.message });
  }
});

app.get('/health/goodtill', async (_req, res) => {
  try {
    await goodtillPing();
    res.json({ status: 'ok', goodtill: 'ok' });
  } catch (err) {
    res.status(503).json({ status: 'error', goodtill: 'error', message: err.message });
  }
});

// Guest Folio sales the bridge could not post (no customer / room / checked-in
// reservation). Unresolved entries are retried automatically for a bounded
// window; whatever remains here needs a human (re-key on MEWS or write off).
// Token-gated: the payload contains guest names and spend, unlike the other
// no-auth endpoints which return no data.
app.get('/dead-letter', (req, res) => {
  const token = process.env.BRIDGE_ADMIN_TOKEN || '';
  if (!token || req.get('X-Bridge-Token') !== token) {
    return res.status(403).json({ error: 'forbidden — set BRIDGE_ADMIN_TOKEN and send it as X-Bridge-Token' });
  }
  const entries = getDeadLetterEntries();
  res.json({ count: entries.length, entries });
});

// ─── Manual triggers ──────────────────────────────────────────────────

app.post('/poll', async (_req, res) => {
  try {
    await pollOnce();
    res.json({ status: 'ok', message: 'Poll cycle completed' });
  } catch (err) {
    res.status(500).json({ status: 'error', message: err.message });
  }
});

app.post('/sync', async (_req, res) => {
  try {
    await fullSync(getRoomMap(), getResourceToRoom());
    res.json({ status: 'ok', message: 'Room roster sync completed' });
  } catch (err) {
    res.status(500).json({ status: 'error', message: err.message });
  }
});

// ─── MEWS Webhook ─────────────────────────────────────────────────────

app.post('/webhooks/mews', async (req, res) => {
  // Optional signature verification
  if (WEBHOOK_SECRET) {
    const sig = req.get('X-Mews-Signature');
    if (sig) {
      const payload = JSON.stringify(req.body);
      const expected = crypto.createHmac('sha256', WEBHOOK_SECRET).update(payload).digest('hex');
      if (!crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(sig))) {
        return res.status(401).send('Invalid signature');
      }
    }
  }

  // Respond immediately — process asynchronously
  res.status(200).send('OK');

  const events = req.body?.Events || [];
  const reservationIds = new Set();

  for (const evt of events) {
    if (evt.Discriminator === 'ServiceOrderUpdated') {
      reservationIds.add(evt.Value?.Id);
    }
  }

  if (reservationIds.size === 0) return;

  console.log(`[webhook] Received ${reservationIds.size} ServiceOrderUpdated event(s)`);

  const roomMap = getRoomMap();
  const resourceToRoom = getResourceToRoom();

  for (const id of reservationIds) {
    try {
      await handleReservationUpdate(id, roomMap, resourceToRoom);
    } catch (err) {
      console.error(`[webhook] Error handling reservation ${id}:`, err.message);
    }
  }
});

// ─── Goodtill Webhook ────────────────────────────────────────────────

app.post('/webhooks/goodtill', async (req, res) => {
  // Verify the request is from Goodtill
  if (GOODTILL_WEBHOOK_TOKEN) {
    const token = req.get('Goodtill-Verification-Token');
    if (token !== GOODTILL_WEBHOOK_TOKEN) {
      return res.status(401).send('Invalid token');
    }
  }

  // Respond immediately — process asynchronously
  res.status(200).send('OK');

  const body = req.body || {};
  const event = body.event;
  if (!event || (!event.startsWith('sale.'))) return; // ignore non-sale events

  // Sale ID is nested under data.sale.id
  const id = body.data?.sale?.id;
  if (!id) {
    console.warn(`[goodtill-webhook] ${event} — no sale ID in payload:`, JSON.stringify(body.data));
    return;
  }

  console.log(`[goodtill-webhook] Received ${event} for sale ${id}`);

  try {
    if (event === 'sale.voided') {
      await handleVoidedSale(id);
    } else if (event === 'sale.completed') {
      await handleCompletedSale(id);
    }
  } catch (err) {
    console.error(`[goodtill-webhook] Error handling ${event} for ${id}:`, err.message);
  }
});

// ─── Startup ──────────────────────────────────────────────────────────

let resyncTimer = null;

async function start() {
  const required = [
    'MEWS_CLIENT_TOKEN',
    'MEWS_ACCESS_TOKEN',
    'GOODTILL_SUBDOMAIN',
    'GOODTILL_USERNAME',
    'GOODTILL_PASSWORD',
  ];
  const missing = required.filter((k) => !process.env[k]);
  if (missing.length) {
    console.error(`[server] Missing required environment variables: ${missing.join(', ')}`);
    process.exit(1);
  }

  // Start the HTTP server FIRST so Railway sees a healthy process
  // Bind to 0.0.0.0 so Railway's proxy can reach the app
  app.listen(PORT, '0.0.0.0', () => {
    console.log(`[server] POS-to-Folio bridge running on 0.0.0.0:${PORT}`);
    console.log(`[server] Health:       GET  http://localhost:${PORT}/health`);
    console.log(`[server] Manual poll:  POST http://localhost:${PORT}/poll`);
    console.log(`[server] Manual sync:  POST http://localhost:${PORT}/sync`);
    console.log(`[server] MEWS webhook: POST http://localhost:${PORT}/webhooks/mews`);
    console.log(`[server] Goodtill webhook: POST http://localhost:${PORT}/webhooks/goodtill`);
  });

  // Then attempt to initialise MEWS config + roster sync (non-fatal)
  try {
    await init();
    startPolling();
    console.log('[server] Bridge initialised and polling started');
  } catch (err) {
    console.error('[server] Init failed (will retry on next re-sync):', err.message);
  }

  // Periodic full re-sync as a safety net (every 10 minutes)
  const resyncInterval = parseInt(process.env.ROSTER_RESYNC_INTERVAL_MS || '600000', 10);
  resyncTimer = setInterval(async () => {
    try {
      // If init hasn't completed, try again
      if (!getRoomMap().size) {
        console.log('[server] Re-attempting init...');
        await init();
        startPolling();
      }
      await fullSync(getRoomMap(), getResourceToRoom());
    } catch (err) {
      console.error('[server] Roster re-sync error:', err.message);
    }
  }, resyncInterval);
}

// ─── Graceful shutdown ────────────────────────────────────────────────

function shutdown(signal) {
  console.log(`[server] ${signal} received, shutting down...`);
  stopPolling();
  if (resyncTimer) clearInterval(resyncTimer);
  process.exit(0);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

start();
