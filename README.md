# POS-to-Folio Bridge

A standalone service that bridges **Goodtill/SumUp POS** "Guest Folio" payments to **MEWS PMS** room folios via the MEWS Connector API.

It also **syncs occupied rooms as customer profiles** on the POS, so waiters can only select rooms with checked-in guests — no manual room-number entry, no charging to empty rooms.

## How It Works

### Room Roster Sync (MEWS → Goodtill)

1. On startup, the bridge **pulls all checked-in reservations** from MEWS
2. For each occupied room it **creates (or activates) a Goodtill customer** named `"Room 101 — John Smith"`
3. When a guest **checks out**, the customer profile is **deactivated** (hidden from the iPad)
4. MEWS **webhooks** (`ServiceOrderUpdated`) trigger real-time updates; a periodic re-sync every 10 minutes acts as a safety net

### Charge Posting (Goodtill → MEWS)

1. **Polls Goodtill** every minute for completed sales with "Guest Folio" payment
2. **Matches the sale's `customer_id`** to the room roster (falls back to parsing the customer name)
3. **Looks up the room** in MEWS and finds the active reservation
4. **Posts an order** to the guest's folio with the itemised POS charges

## Staff Workflow

1. Ring up the bill in SumUp/Goodtill as usual
2. When the guest asks to charge it to their room, **tap "Guest Folio"** as the payment method
3. **Pick the guest's room from the customer list** (e.g. "Room 101 — John Smith") — only occupied rooms appear
4. Complete the sale
5. The bridge posts the charge to the guest's MEWS folio within ~1 minute

> Staff never type room numbers. They pick from a pre-populated list of occupied rooms, which the bridge keeps in sync with MEWS check-ins / check-outs.

## Setup

```bash
# 1. Install dependencies
npm install

# 2. Copy and configure environment variables
cp .env.example .env
# Edit .env with your MEWS and Goodtill credentials

# 3. Start the service
npm start

# Or in development with auto-restart on file changes:
npm run dev
```

### Register MEWS Webhook (optional but recommended)

Contact MEWS support (or use the MEWS Commander integration settings) to register a General Webhook pointing to:

```
POST https://<your-domain>/webhooks/mews
```

Subscribe to `ServiceOrderUpdated` events. Without the webhook, the bridge still works — it re-syncs the roster every 10 minutes via polling.

## Configuration

See `.env.example` for all available environment variables. Key settings:

| Variable | Description |
|---|---|
| `MEWS_CLIENT_TOKEN` | Your MEWS Connector API client token |
| `MEWS_ACCESS_TOKEN` | Your MEWS Connector API access token |
| `MEWS_WEBHOOK_SECRET` | Optional: secret for HMAC-SHA256 webhook verification |
| `GOODTILL_SUBDOMAIN` | Your Goodtill/SumUp subdomain |
| `GOODTILL_USERNAME` | Goodtill API username |
| `GOODTILL_PASSWORD` | Goodtill API password |
| `POLL_INTERVAL_MS` | Polling interval in ms (default: 60000) |
| `ROSTER_RESYNC_INTERVAL_MS` | Full roster re-sync interval (default: 600000 / 10 min) |
| `MEWS_FB_SERVICE_ID` | Optional: explicit F&B service ID in MEWS |
| `MEWS_FB_ACCOUNTING_CATEGORY_ID` | Optional: explicit accounting category ID |

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check — returns uptime |
| `POST` | `/poll` | Trigger a manual Goodtill poll cycle |
| `POST` | `/sync` | Trigger a manual full roster sync |
| `POST` | `/webhooks/mews` | MEWS webhook receiver (ServiceOrderUpdated) |

## Architecture

```
src/
├── index.js      — Express server, webhook endpoint, startup
├── bridge.js     — Orchestrator: poll → filter → match → post
├── roster.js     — Room roster sync (MEWS ↔ Goodtill customers)
├── goodtill.js   — Goodtill/SumUp POS API client (sales + customers)
├── mews.js       — MEWS Connector API client
└── store.js      — Processed-sales dedup tracker (file-based)
```

## Duplicate Prevention & Void Reversal

Processed sales are tracked in `data/processed-sales.json` (and unpostable sales in `data/dead-letter.json`). This store is the bridge's **only** record of which POS sale maps to which MEWS order, so two things depend on it:

- **No double-posting** — a sale already in the store is never posted twice.
- **Void reversal** — when a sale is voided in SumUp, `detectVoids()` finds its stored MEWS order and cancels it. If the store is lost, the bridge no longer knows which MEWS order to cancel, and **the void silently fails to reverse**.

> ⚠️ **The store must live on persistent storage.** The `data/` directory is gitignored, and on a host with an ephemeral filesystem (e.g. Railway without a volume) **every redeploy or restart wipes it**. Set `DATA_DIR` to a mounted volume in production.
>
> **Railway:** add a Volume to the service, set its mount path to `/data`, and set `DATA_DIR=/data`. Verify with `GET /health` — `store.dataDir` should read `/data` and `store.processedSales` should stay non-zero across deploys (a reset to `0` after each deploy means the volume isn't mounted).

Bridge-managed Goodtill customer profiles are tagged with `source: "mews-bridge"` and `custom_field_1: "mews:<reservationId>"` to distinguish them from manually-created customers.

## Troubleshooting

- **"Room X not found in MEWS"** — The room number doesn't match any MEWS resource name. Check room naming conventions.
- **"No checked-in reservation found"** — The room exists but no guest is currently checked in. Verify in MEWS.
- **"Could not find F&B service"** — Set `MEWS_FB_SERVICE_ID` explicitly in `.env`.
- **Rooms not appearing on iPad** — Trigger a manual sync via `POST /sync` and check logs. Ensure Goodtill credentials are correct.
- **Payment method not detected** — The code looks for payment types containing "guest folio" or "room charge" (case-insensitive). Verify the exact payment method name in SumUp.
