/**
 * Room Roster Sync
 *
 * Maintains Goodtill customer profiles that mirror MEWS checked-in rooms.
 * Each occupied room gets an active customer like "Room 101 — John Smith".
 * When a guest checks out, the customer is deactivated (hidden from iPad).
 *
 * Waiters pick from the active customer list on the iPad — they can ONLY
 * select occupied rooms because checked-out rooms are deactivated.
 */

const {
  listCustomers,
  createCustomer,
  updateCustomer,
  activateCustomer,
  deactivateCustomer,
} = require('./goodtill');
const {
  getResourcesAndRoomMap,
  getAllCheckedInReservations,
  getActiveReservationForRoom,
  getCustomers,
} = require('./mews');

// Prefix used to identify bridge-managed customers in Goodtill
const CUSTOMER_PREFIX = 'Room ';
const MANAGED_SOURCE = 'mews-bridge';

// Goodtill serializes the active flag inconsistently (1 vs "1" vs true)
const isActiveFlag = (c) => c.active === 1 || c.active === '1' || c.active === true;

/**
 * Pick the ONE record that represents a room, deterministically.
 *
 * Never pick by response order: Goodtill returns /customers sorted by
 * updated_at ascending, so "first match wins" selects the STALEST record and
 * rotates the chosen identity every sync — renaming ancient records to the
 * current guest, flip-flopping active flags (~2 writes per room per sync),
 * and littering the POS iPad's cached customer list with past guests.
 *
 * Preference: record already stamped with the room's current reservation
 * (active first), then the currently-active record, then the most recently
 * updated. Steady state re-chooses the same record and writes nothing.
 *
 * @param {any[]} records - all bridge-managed Goodtill customers for a room
 * @param {string|null} mewsRef - "mews:<reservationId>" of the current stay
 */
function chooseRoomRecord(records, mewsRef) {
  const latest = (a, b) => ((a.updated_at || '') >= (b.updated_at || '') ? a : b);
  const refMatches = mewsRef ? records.filter((c) => c.custom_field_1 === mewsRef) : [];
  const activeRefMatches = refMatches.filter(isActiveFlag);
  if (activeRefMatches.length) return activeRefMatches.reduce(latest);
  if (refMatches.length) return refMatches.reduce(latest);
  const actives = records.filter(isActiveFlag);
  if (actives.length) return actives.reduce(latest);
  return records.reduce(latest);
}

/**
 * In-memory map of room number → Goodtill customer ID
 * @type {Map<string, string>}
 */
const roomCustomerMap = new Map();

/**
 * In-memory map of Goodtill customer ID → { roomNumber, reservationId }
 * @type {Map<string, { roomNumber: string, reservationId: string }>}
 */
const customerRoomMap = new Map();

/**
 * Full sync: compare MEWS checked-in reservations against Goodtill customers
 * and create/activate/deactivate as needed.
 *
 * @param {Map<string, string>} roomMap - room name → MEWS resource ID
 * @param {Map<string, string>} resourceToRoom - MEWS resource ID → room name
 */
async function fullSync(roomMap, resourceToRoom) {
  console.log('[roster] Starting full sync...');

  // 1. Get all checked-in reservations from MEWS
  const reservations = await getAllCheckedInReservations();
  console.log(`[roster] Found ${reservations.length} checked-in reservations in MEWS`);

  // 2. Build a map of room → { reservation, guestName }
  const occupiedRooms = new Map(); // roomNumber → { reservation, resourceId }
  for (const r of reservations) {
    const resIds = r.AssignedResourceIds || (r.AssignedResourceId ? [r.AssignedResourceId] : []);
    for (const resId of resIds) {
      const roomNumber = resourceToRoom.get(resId);
      if (roomNumber) {
        occupiedRooms.set(roomNumber, { reservation: r, resourceId: resId });
      }
    }
  }

  // 3. Get guest names for all reservations (may fail if token lacks permission)
  const customerIds = [...new Set(reservations.map((r) => r.AccountId || r.CustomerId).filter(Boolean))];
  const customerNameMap = new Map();
  try {
    const mewsCustomers = customerIds.length ? await getCustomers(customerIds) : [];
    for (const c of mewsCustomers) {
      const name = [c.FirstName, c.LastName].filter(Boolean).join(' ') || 'Guest';
      customerNameMap.set(c.Id, name);
    }
  } catch (err) {
    console.warn(`[roster] Could not fetch guest names from MEWS (${err.message}). Using "Guest" as fallback.`);
  }

  // 4. Get all existing bridge-managed customers from Goodtill
  const allGtCustomers = await listCustomers();
  const managedCustomers = allGtCustomers.filter(
    (c) => c.source === MANAGED_SOURCE || c.custom_field_1?.startsWith('mews:')
  );
  console.log(`[roster] Found ${managedCustomers.length} bridge-managed customers in Goodtill`);

  // Group by room code, then choose ONE record per room (see chooseRoomRecord —
  // response order rotates, so the choice must not depend on it)
  const recordsByRoom = new Map();
  for (const c of managedCustomers) {
    const match = c.name?.match(/^(?:Room\s+)?(\S+)\s*—/i);
    if (match) {
      const room = match[1];
      if (!recordsByRoom.has(room)) recordsByRoom.set(room, []);
      recordsByRoom.get(room).push(c);
    }
  }
  const gtByRoom = new Map();
  const duplicates = [];
  for (const [room, records] of recordsByRoom) {
    const currentRes = occupiedRooms.get(room)?.reservation;
    const chosen = chooseRoomRecord(records, currentRes ? `mews:${currentRes.Id}` : null);
    gtByRoom.set(room, chosen);
    for (const c of records) {
      if (c !== chosen) duplicates.push(c);
    }
  }
  // Only deactivate duplicates that are still active. Historical checked-out
  // customers are already inactive — re-deactivating them on every sync makes
  // thousands of pointless API calls and can stall the sync before steps 5/6
  // run (so current guests never get pushed to Goodtill).
  const toDeactivate = duplicates.filter(isActiveFlag);
  if (toDeactivate.length) {
    console.log(`[roster] Cleaning up ${toDeactivate.length} duplicate customers (${duplicates.length - toDeactivate.length} already inactive, skipped)...`);
    for (const dup of toDeactivate) {
      await deactivateCustomer(dup.id);
    }
  }

  // 5. For each occupied room: create or activate Goodtill customer
  for (const [roomNumber, { reservation }] of occupiedRooms) {
    const guestCustId = reservation.AccountId || reservation.CustomerId;
    const guestName = customerNameMap.get(guestCustId) || 'Guest';
    const displayName = `${roomNumber} — ${guestName}`;
    const mewsRef = `mews:${reservation.Id}`;

    const existing = gtByRoom.get(roomNumber);
    if (existing) {
      // Update name/activation/reservation ref if needed
      const needsUpdate =
        existing.name !== displayName ||
        !isActiveFlag(existing) ||
        existing.custom_field_1 !== mewsRef;
      if (needsUpdate) {
        console.log(`[roster] Updating customer for room ${roomNumber}: "${displayName}"`);
        await updateCustomer(existing.id, {
          name: displayName,
          active: 1,
          custom_field_1: mewsRef,
          source: MANAGED_SOURCE,
        });
      }
      roomCustomerMap.set(roomNumber, existing.id);
      customerRoomMap.set(existing.id, { roomNumber, reservationId: reservation.Id });
      gtByRoom.delete(roomNumber);
    } else {
      // Create new customer
      console.log(`[roster] Creating customer for room ${roomNumber}: "${displayName}"`);
      const newCust = await createCustomer({
        name: displayName,
        active: 1,
        custom_field_1: mewsRef,
        source: MANAGED_SOURCE,
      });
      if (newCust?.id) {
        roomCustomerMap.set(roomNumber, newCust.id);
        customerRoomMap.set(newCust.id, { roomNumber, reservationId: reservation.Id });
      }
    }
  }

  // 6. Deactivate customers for rooms no longer occupied
  for (const [roomNumber, c] of gtByRoom) {
    if (isActiveFlag(c)) {
      console.log(`[roster] Deactivating customer for room ${roomNumber} (checked out)`);
      await deactivateCustomer(c.id);
    }
    // Keep in maps for reference but mark as gone
    customerRoomMap.delete(c.id);
  }

  console.log(`[roster] Full sync complete: ${occupiedRooms.size} rooms occupied, ${roomCustomerMap.size} customers active`);
}

/**
 * Handle a MEWS ServiceOrderUpdated webhook event.
 * Fetches the reservation details and syncs the corresponding Goodtill customer.
 *
 * @param {string} reservationId - The MEWS reservation ID from the webhook
 * @param {Map<string, string>} roomMap - room name → MEWS resource ID
 * @param {Map<string, string>} resourceToRoom - MEWS resource ID → room name
 */
async function handleReservationUpdate(reservationId, roomMap, resourceToRoom) {
  // Fetch the reservation details
  const { getAll, getCustomers: getMewsCustomers } = require('./mews');
  const reservations = await getAll('reservations/getAll/2023-06-06', {
    ReservationIds: [reservationId],
    Extent: { Reservations: true },
  }, 'Reservations');

  if (!reservations.length) {
    console.warn(`[roster] Reservation ${reservationId} not found`);
    return;
  }

  const reservation = reservations[0];
  const state = reservation.State;
  const resIds = reservation.AssignedResourceIds || (reservation.AssignedResourceId ? [reservation.AssignedResourceId] : []);

  for (const resId of resIds) {
    const roomNumber = resourceToRoom.get(resId);
    if (!roomNumber) continue;

    if (state === 'Started') {
      // Check-in: activate/create customer
      const guestCustId = reservation.AccountId || reservation.CustomerId;
      let guestName = 'Guest';
      if (guestCustId) {
        try {
          const guests = await getMewsCustomers([guestCustId]);
          if (guests.length) {
            guestName = [guests[0].FirstName, guests[0].LastName].filter(Boolean).join(' ') || 'Guest';
          }
        } catch (err) {
          console.warn(`[roster] Could not fetch guest name (${err.message}). Using "Guest".`);
        }
      }

      const displayName = `${roomNumber} — ${guestName}`;
      const mewsRef = `mews:${reservation.Id}`;
      const existingId = roomCustomerMap.get(roomNumber);

      if (existingId) {
        console.log(`[roster] Check-in room ${roomNumber}: updating "${displayName}"`);
        await updateCustomer(existingId, {
          name: displayName,
          active: 1,
          custom_field_1: mewsRef,
          source: MANAGED_SOURCE,
        });
        customerRoomMap.set(existingId, { roomNumber, reservationId: reservation.Id });
      } else {
        console.log(`[roster] Check-in room ${roomNumber}: creating "${displayName}"`);
        const newCust = await createCustomer({
          name: displayName,
          active: 1,
          custom_field_1: mewsRef,
          source: MANAGED_SOURCE,
        });
        if (newCust?.id) {
          roomCustomerMap.set(roomNumber, newCust.id);
          customerRoomMap.set(newCust.id, { roomNumber, reservationId: reservation.Id });
        }
      }
    } else if (state === 'Processed' || state === 'Canceled' || state === 'Optional') {
      // MEWS fires ServiceOrderUpdated for *any* reservation change, including old
      // processed ones. Only deactivate when the map entry actually points to THIS
      // reservation — otherwise we'd deactivate the current guest's customer whenever
      // a prior checked-out reservation in the same room gets touched.
      const existingId = roomCustomerMap.get(roomNumber);
      const mapped = existingId ? customerRoomMap.get(existingId) : null;
      if (existingId && mapped?.reservationId === reservation.Id) {
        console.log(`[roster] Check-out room ${roomNumber}: deactivating customer`);
        await deactivateCustomer(existingId);
        customerRoomMap.delete(existingId);
        roomCustomerMap.delete(roomNumber);
      } else if (existingId) {
        console.log(`[roster] Ignoring ${state} event for reservation ${reservation.Id} (room ${roomNumber}) — current occupant is a different reservation`);
      }
    }
  }
}

/**
 * Look up room info from a Goodtill customer ID (from a sale)
 * @param {string} gtCustomerId - Goodtill customer ID
 * @returns {{ roomNumber: string, reservationId: string } | null}
 */
function getRoomByCustomerId(gtCustomerId) {
  return customerRoomMap.get(gtCustomerId) || null;
}

/**
 * Get the Goodtill customer ID for a room number
 * @param {string} roomNumber
 * @returns {string | null}
 */
function getCustomerIdByRoom(roomNumber) {
  return roomCustomerMap.get(roomNumber) || null;
}

module.exports = {
  fullSync,
  handleReservationUpdate,
  getRoomByCustomerId,
  getCustomerIdByRoom,
  roomCustomerMap,
  customerRoomMap,
};
