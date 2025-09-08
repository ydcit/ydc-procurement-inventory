/* ========= YDC Procurement Inventory (Apps Script) =========
 * Single-approval flow + configurable notifications.
 * Emails use a centered "card" layout and link to the Web App URL.
 */

const APP_NAME = 'YDC Procurement Inventory';

const SCRIPTPROP = PropertiesService.getScriptProperties();
const PROP_SSID   = 'YDC_PROC_SSID';
const P_SKU       = 'COUNTER_SKU';
const P_TRX       = 'COUNTER_TRX';
const P_USER      = 'COUNTER_USER';

const SHEET_USERS   = 'Users';
const SHEET_ITEMS   = 'Items';
const SHEET_PENDING = 'Pending';
const SHEET_LEDGER  = 'Ledger';
const SHEET_NOTIFY  = 'Notifications';

// Notification event keys
const NE = {
  PENDING:      'PENDING',      // Controllers only + requester gets separate "submitted" notice
  RECEIVE:      'RECEIVE',
  ISSUE:        'ISSUE',
  CREATE_SKU:   'CREATE_SKU',
  MODIFY_SKU:   'MODIFY_SKU',
  RETIRE_SKU:   'RETIRE_SKU',
  USER_CREATED: 'USER_CREATED',
  LOW_STOCK:    'LOW_STOCK',
  DAILY_KPI:    'DAILY_KPI'
};

function pad(n, len) { return String(n).padStart(len, '0'); }
function nowISO() { return new Date().toISOString(); }

/* ---------------- Spreadsheet wiring ---------------- */
function getSS() {
  const id = SCRIPTPROP.getProperty(PROP_SSID);
  if (!id) throw new Error('Spreadsheet not initialized. Run setup() or connectToSpreadsheet(ssId).');
  return SpreadsheetApp.openById(id);
}
function sheet(name) {
  const sh = getSS().getSheetByName(name);
  if (!sh) throw new Error('Missing sheet: ' + name);
  return sh;
}
function ensureColumns(sh, required) {
  const lastCol = Math.max(1, sh.getLastColumn());
  const hdr = sh.getRange(1, 1, 1, lastCol).getValues()[0];
  const have = new Set(hdr.filter(Boolean));
  let curLast = lastCol;
  required.forEach(col => {
    if (!have.has(col)) {
      sh.insertColumnAfter(curLast);
      curLast = sh.getLastColumn();
      sh.getRange(1, curLast, 1, 1).setValue(col);
    }
  });
}
function ensureSheets(ss) {
  const want = {
    [SHEET_USERS]:   ['UserID','Email','Name','Department','RequestedRole','Role','Status','CreatedAt'],
    [SHEET_ITEMS]:   ['SKU','Name','Description','UoM','Location','Qty','Status','CreatedAt','UpdatedAt'],
    [SHEET_PENDING]: ['PendingID','LinkID','When','Type','SKU','Name','UoM','Qty','Delta','Reason','Note','By','Status','PayloadJSON'],
    [SHEET_LEDGER]:  ['ID','When','Type','SKU','Item','Delta','UoM','Status','By','Note'],
    [SHEET_NOTIFY]:  ['Event','Enabled','Recipients','CC','Threshold','Hour','Note']
  };

  Object.keys(want).forEach(name => {
    let sh = ss.getSheetByName(name);
    if (!sh) sh = ss.insertSheet(name);

    if (sh.getLastRow() === 0) {
      sh.appendRow(want[name]);                 // header
    } else {
      ensureColumns(sh, want[name]);            // ensure columns exist
    }

    // Seed notification routes if header-only
    if (name === SHEET_NOTIFY && sh.getLastRow() === 1) {
      const rows = [
        ['RECEIVE',       true, '', '', '',  '', 'When item was inbound (approved)'],
        ['ISSUE',         true, '', '', '',  '', 'When item was outbound (approved)'],
        ['CREATE_SKU',    true, '', '', '',  '', 'When a SKU is created (approved)'],
        ['MODIFY_SKU',    true, '', '', '',  '', 'When an item is modified (approved)'],
        ['RETIRE_SKU',    true, '', '', '',  '', 'When an item is retired (approved)'],
        ['PENDING',       true, '', '', '',  '', 'Controllers get approval requests (requester gets a separate notice)'],
        ['USER_CREATED',  true, '', '', '',  '', 'When a new user was created'],
        ['LOW_STOCK',     true, '', '', 10,  '', 'Low stock alert; Threshold = min qty'],
        ['DAILY_KPI',     true, '', '', '',   9, 'Daily KPI email; Hour = 0–23 (account tz)']
      ];
      sh.getRange(2, 1, rows.length, rows[0].length).setValues(rows);
    }
  });
}

function setup() {
  const ss = SpreadsheetApp.create('YDC Procurement Inventory — Data');
  SCRIPTPROP.setProperty(PROP_SSID, ss.getId());
  ensureSheets(ss);

  const owner = Session.getEffectiveUser().getEmail() || 'owner@domain.tld';
  const users = ss.getSheetByName(SHEET_USERS);
  if (users.getLastRow() === 1) {
  const uid = 'USR-' + pad(nextCounter(P_USER), 5);
  _append(users, {
    UserID: uid,
    Email: owner,
    Name: 'System Controller',
    Department: 'IT',
    RequestedRole: '',
    Role: 'controller',
    Status: 'Active',
    CreatedAt: nowISO()
  });
}
}
function _parseSpreadsheetId(input) {
  const s = String(input || '').trim();
  const m = s.match(/[-\w]{25,}/); // tolerant: Drive file IDs
  if (!m) throw new Error('connectToSpreadsheet: Paste only the Spreadsheet ID or its full URL.');
  return m[0];
}
function connectToSpreadsheet(ssIdOrUrl) {
  const id = _parseSpreadsheetId(ssIdOrUrl);
  const ss = SpreadsheetApp.openById(id);
  ensureSheets(ss);
  SCRIPTPROP.setProperty(PROP_SSID, id);
  return { ok:true, id, url:ss.getUrl(), name:ss.getName() };
}
function getSpreadsheetInfo() {
  const ss = getSS();
  return { id:ss.getId(), url:ss.getUrl(), name:ss.getName() };
}

/* ---------------- Helpers ---------------- */
function _readObjects(sh) {
  const vals = sh.getDataRange().getValues();
  if (vals.length < 2) return [];
  const hdr = vals[0];
  return vals.slice(1).map(r => Object.fromEntries(hdr.map((h, i) => [h, r[i]])));
}
function _append(sh, obj) {
  const hdr = sh.getRange(1,1,1,Math.max(1, sh.getLastColumn())).getValues()[0];
  const row = hdr.map(h => (h in obj) ? obj[h] : '');
  sh.appendRow(row);
}
function _updateByKey(sh, keyField, keyValue, patch) {
  const data = _readObjects(sh);
  const hdr = sh.getRange(1,1,1,Math.max(1, sh.getLastColumn())).getValues()[0];
  const idx = data.findIndex(x => String(x[keyField]) === String(keyValue));
  if (idx < 0) return false;
  const row = idx + 2;
  const existing = data[idx];
  const updated = {...existing, ...patch};
  const out = hdr.map(h => (h in updated) ? updated[h] : '');
  sh.getRange(row, 1, 1, hdr.length).setValues([out]);
  return true;
}
function _findBy(sh, keyField, keyValue) {
  return _readObjects(sh).find(x => String(x[keyField]) === String(keyValue)) || null;
}

/* ---------------- Counters ---------------- */
function nextCounter(key) {
  const current = Number(SCRIPTPROP.getProperty(key) || '0') + 1;
  SCRIPTPROP.setProperty(key, String(current));
  return current;
}
function nextSkuId() { return 'YDC-PROC-' + pad(nextCounter(P_SKU), 4); }
function nextTrxId() { return 'YDC-PROC-TRX-' + pad(nextCounter(P_TRX), 6); }

/* ---------------- Auth & Users ---------------- */
function getActiveEmail() { return Session.getActiveUser().getEmail() || ''; }
function getCurrentUser() {
  const email = getActiveEmail();
  if (!email) return { email:'', status:'NoSession' };
  const u = _readObjects(sheet(SHEET_USERS)).find(x => String(x.Email).toLowerCase() === email.toLowerCase());
  if (!u) return { email, status:'Unknown' };
  return { email, name:u.Name, department:u.Department, requestedRole:u.RequestedRole, role:u.Role, status:u.Status, userId:u.UserID };
}
function requestAccount(name, department, requestedRole) {
  const email = getActiveEmail();
  if (!email) throw new Error('No signed-in Google account detected.');

  const sh = sheet(SHEET_USERS);
  // Make sure the column exists (adds if missing)
  ensureColumns(sh, ['UserID','Email','Name','Department','RequestedRole','Role','Status','CreatedAt']);

  const existing = _readObjects(sh).find(r => String(r.Email).toLowerCase() === String(email).toLowerCase());
  if (existing) {
    return { ok:false, message:'Account already exists with status: ' + (existing.Status || 'Unknown') };
  }

  const uid = 'USR-' + pad(nextCounter(P_USER), 5);
  _append(sh, {
    UserID: uid,
    Email: email,
    Name: name || email,
    Department: department || '',
    RequestedRole: requestedRole || 'user',
    Role: 'user',
    Status: 'Pending',
    CreatedAt: nowISO() // <-- this drives the “When” column in the UI
  });

  notifyUserCreated({ email, name, department, requestedRole });
  return { ok:true, message:'Account request submitted. Wait for Controller approval.' };
}
function listUsers() {
  const me = getCurrentUser();
  if (me.role !== 'controller' || me.status !== 'Active') return [];
  return _readObjects(sheet(SHEET_USERS));
}
function setUserStatus(userId, role, status) {
  const me = getCurrentUser();
  if (me.role !== 'controller' || me.status !== 'Active') throw new Error('Only controllers can update users.');
  const ok = _updateByKey(sheet(SHEET_USERS), 'UserID', userId, { Role: role, Status: status });
  return { ok };
}

/* ---------------- Data API ---------------- */
function getItems() { return _readObjects(sheet(SHEET_ITEMS)); }
function getLedger(limit) {
  const data = _readObjects(sheet(SHEET_LEDGER));
  if (!limit) return data;
  return data.slice(Math.max(0, data.length - limit));
}
function getPending() {
  const me = getCurrentUser();
  // Managers can see/approve transactions; only controllers manage users
  if (!['controller','manager'].includes(me.role) || me.status !== 'Active') return [];
  return _readObjects(sheet(SHEET_PENDING)).filter(p => p.Status === 'Pending');
}
function getCounts() {
  const items = getItems().filter(x => x.Status !== 'Retired');
  const pending = _readObjects(sheet(SHEET_PENDING)).filter(p => p.Status === 'Pending').length;
  const ledger = _readObjects(sheet(SHEET_LEDGER)).length;
  return { activeSkus: items.length, onhand: items.reduce((a,b)=> a + Number(b.Qty||0), 0), pending, ledger };
}
function getMyActivity() {
  const me = getCurrentUser();
  const email = (me.email || '').toLowerCase();
  const allPending = _readObjects(sheet(SHEET_PENDING));
  const allLedger  = _readObjects(sheet(SHEET_LEDGER));

  const minePending = allPending.filter(r => String(r.By || '').toLowerCase() === email);
  const mineLedger  = allLedger.filter(r => String(r.By || '').toLowerCase() === email);

  return { minePending, mineLedger };
}

function getBootstrap() {
  // Make columns E..H = RequestedRole, Role, Status, CreatedAt
  normalizeUsersSheet();
  ensureUsersTimestampColumnAndBackfill(); // will now fill column H only

  const me = getCurrentUser();
  const info = getSpreadsheetInfo();

  const usersPending = (me.role === 'controller' && me.status === 'Active')
    ? _readObjects(sheet(SHEET_USERS)).filter(u => u.Status === 'Pending')
    : [];

  const my = getMyActivity();

  return {
    user: me,
    counts: getCounts(),
    items: getItems(),
    pending: getPending(),
    usersPending,
    ledger: getLedger(500),
    minePending: my.minePending,
    mineLedger:  my.mineLedger,
    db: info
  };
}


/* ---------------- Queue Pending ---------------- */
function queuePending(rec) {
  const me = getCurrentUser();
  if (!me.email || me.status !== 'Active') throw new Error('Not authorized.');

  const linkId = nextTrxId();
  const pendingId = linkId + '-P';

  _append(sheet(SHEET_LEDGER), {
    ID: linkId, When: nowISO(), Type: rec.type, SKU: rec.sku || '', Item: rec.name || '',
    Delta: rec.delta || 0, UoM: rec.uom || '', Status: 'Pending', By: me.email, Note: rec.note || ''
  });
  _append(sheet(SHEET_PENDING), {
    PendingID: pendingId, LinkID: linkId, When: nowISO(), Type: rec.type, SKU: rec.sku || '',
    Name: rec.name || '', UoM: rec.uom || '', Qty: rec.qty || 0, Delta: rec.delta || 0,
    Reason: rec.reason || '', Note: rec.note || '', By: me.email, Status: 'Pending',
    PayloadJSON: rec.payload ? JSON.stringify(rec.payload) : ''
  });

  // Controllers only + a separate "submitted" notice to requester
  notifyPendingCreated({
    linkId, pendingId,
    rec,
    by: me.email
  });

  return { ok:true, pendingId, linkId };
}

/* ---------------- Actions ---------------- */
function actionCreateSku(payload) {
  if (!payload) throw new Error('Missing payload');
  if (!payload.sku) payload.sku = nextSkuId();
  return queuePending({
    type:'CREATE_SKU', sku:payload.sku, name:payload.name, uom:payload.uom,
    note:`Create ${payload.sku} — ${payload.name}`, payload
  });
}

function actionModifySku(payload) {
  if (!payload || !payload.sku) throw new Error('Missing SKU');

  const it = _findBy(sheet(SHEET_ITEMS), 'SKU', payload.sku);
  if (!it) throw new Error('SKU not found');

  // Allow changing Status between Active <-> On Hold via Modify (retire stays a separate action)
  const fields = [
    ['Name','name'],
    ['Description','desc'],
    ['UoM','uom'],
    ['Location','loc'],
    ['Status','status'] // NEW
  ];

  const changes = [];
  fields.forEach(([col,key])=>{
    const from = it[col] ?? '';
    let to   = (payload[key] ?? from);

    if (col === 'Status') {
      if (String(it.Status) === 'Retired') {
        throw new Error('Cannot modify a Retired item. Receive stock (reactivate) before changes.');
      }
      if (to === '' || to === null || to === undefined) to = from;
      if (String(to) === 'Retired') {
        throw new Error('Use the Retire SKU action to retire an item.');
      }
      if (!['Active','On Hold'].includes(String(to))) {
        throw new Error('Status must be either "Active" or "On Hold".');
      }
    }

    if (String(from) !== String(to)) changes.push({ field: col, from, to });
  });

  const summary = changes.length
    ? changes.map(c => `${c.field}: “${(c.from ?? '') || '—'}” → “${(c.to ?? '') || '—'}”`).join('; ')
    : 'No visible field changes';
  const trimmed = summary.length > 180 ? summary.slice(0,177)+'…' : summary;

  return queuePending({
    type:'MODIFY_SKU',
    sku: payload.sku,
    name: payload.name || it.Name || '',
    uom:  payload.uom  || it.UoM  || '',
    note: `Modify ${payload.sku} — ${trimmed}`,
    payload: { ...payload, changes } // includes payload.status when provided
  });
}

function actionRetireSku(sku, note) {
  if (!sku) throw new Error('Missing SKU');
  const it = _findBy(sheet(SHEET_ITEMS), 'SKU', sku);
  if (!it) throw new Error('SKU not found');
  return queuePending({ type:'RETIRE_SKU', sku, name:it.Name, uom:it.UoM, note: note || `Retire ${sku}`, payload:{status:'Retired'} });
}

function actionReceive(sku, qty, note, reactivateIfRetired) {
  if (!sku || !(qty > 0)) throw new Error('Invalid receive request');
  const it = _findBy(sheet(SHEET_ITEMS), 'SKU', sku);
  if (!it) throw new Error('SKU not found');
  const payload = reactivateIfRetired ? { reactivateIfRetired: true } : null;
  return queuePending({
    type:'RECEIVE', sku, name:it.Name, uom:it.UoM, qty, delta:+qty,
    note: note || `Receive ${qty} ${it.UoM} — ${sku}`, payload
  });
}

function actionIssue(sku, qty, employee, department, reason) {
  if (!sku || !(qty > 0)) throw new Error('Invalid issue request');
  const it = _findBy(sheet(SHEET_ITEMS), 'SKU', sku);
  if (!it) throw new Error('SKU not found');
  if (String(it.Status) === 'On Hold') throw new Error('This item is On Hold and cannot be issued.');
  if (Number(qty) > Number(it.Qty || 0)) throw new Error('Cannot issue more than on-hand quantity');
  const note = `Issue ${qty} ${it.UoM} to ${employee} (${department}). Reason: ${reason || ''}`;
  return queuePending({ type:'ISSUE', sku, name:it.Name, uom:it.UoM, qty, delta:-Math.abs(qty), reason, note });
}

/* ---------------- Approvals ---------------- */
function approvePending(pendingId, commentOpt) {
  const me = getCurrentUser();
  if (!['controller','manager'].includes(me.role) || me.status !== 'Active') {
    throw new Error('Only controllers or managers can approve.');
  }
  const pen = _findBy(sheet(SHEET_PENDING), 'PendingID', pendingId);
  if (!pen) throw new Error('Request not found.');
  if (String(pen.Status) !== 'Pending') {
    throw new Error('This request has already been processed (status: ' + pen.Status + ').');
  }

  const items = sheet(SHEET_ITEMS);
  const type = pen.Type;
  const qty  = Number(pen.Qty || 0);
  const payload = pen.PayloadJSON ? JSON.parse(pen.PayloadJSON) : null;
  const sku = (payload && payload.sku) || pen.SKU;

  // === apply the business effect first (same as before) ===
  if (type === 'CREATE_SKU') {
    const currentRow = _findBy(items, 'SKU', sku);
    const name = (payload && payload.name) || pen.Name || '';
    const desc = (payload && payload.desc) || '';
    const uom  = (payload && payload.uom) || '';
    const loc  = (payload && payload.loc) || '';
    if (currentRow) {
      _updateByKey(items, 'SKU', sku, {
        SKU: sku, Name: name, Description: desc, UoM: uom, Location: loc,
        Qty: Number(currentRow.Qty || 0), Status: currentRow.Status || 'Active',
        CreatedAt: currentRow.CreatedAt || nowISO(), UpdatedAt: nowISO()
      });
    } else {
      _append(items, {
        SKU: sku, Name: name, Description: desc, UoM: uom, Location: loc,
        Qty: 0, Status: 'Active', CreatedAt: nowISO(), UpdatedAt: nowISO()
      });
    }
  } else if (type === 'MODIFY_SKU') {
    const it = _findBy(items, 'SKU', sku);
    if (it) _updateByKey(items, 'SKU', sku, {
      Name:        (payload && payload.name) ?? it.Name,
      Description: (payload && payload.desc) ?? it.Description,
      UoM:         (payload && payload.uom)  ?? it.UoM,
      Location:    (payload && payload.loc)  ?? it.Location,
      Status: (payload && typeof payload.status !== 'undefined')
        ? (['Active','On Hold'].includes(String(payload.status)) ? String(payload.status) : it.Status)
        : it.Status,
      UpdatedAt: nowISO()
    });
  } else if (type === 'RETIRE_SKU') {
    const it = _findBy(items, 'SKU', sku);
    if (it && Number(it.Qty || 0) === 0) {
      _updateByKey(items, 'SKU', sku, { Status:'Retired', UpdatedAt:nowISO() });
    } else {
      throw new Error('Cannot retire: stock must be exactly 0 at approval time.');
    }
  } else if (type === 'RECEIVE') {
    const it = _findBy(items, 'SKU', sku);
    if (it) {
      if (payload && payload.reactivateIfRetired && String(it.Status) === 'Retired') {
        _updateByKey(items, 'SKU', sku, { Status:'Active', UpdatedAt: nowISO() });
        _append(sheet(SHEET_LEDGER), {
          ID: nextTrxId(), When: nowISO(), Type:'REACTIVATE_SKU', SKU: sku, Item: it.Name,
          Delta: '', UoM: it.UoM || '', Status:'Approved', By: me.email,
          Note: 'Auto-reactivate on receive approval'
        });
      }
      _updateByKey(items, 'SKU', sku, { Qty: Number(it.Qty||0) + qty, UpdatedAt: nowISO() });
    }
  } else if (type === 'ISSUE') {
    const it = _findBy(items, 'SKU', sku);
    if (it) {
      const onhand = Number(it.Qty||0);
      if (qty > onhand) throw new Error('Cannot issue more than on-hand at approval time.');
      const newQty = onhand - qty;
      _updateByKey(items, 'SKU', sku, { Qty: newQty, UpdatedAt: nowISO() });
      maybeNotifyLowStock([{ SKU: sku, Name: it.Name, Qty: newQty, UoM: it.UoM, Location: it.Location, Status: it.Status }]);
    }
  }

  // === mark approved and append optional comment to notes ===
  _updateByKey(sheet(SHEET_PENDING), 'PendingID', pendingId, { Status:'Approved' });
  _updateByKey(sheet(SHEET_LEDGER),  'ID',        pen.LinkID,  { Status:'Approved' });

  // carry over original pen.Note into ledger if it was blank/placeholder
  const led = _findBy(sheet(SHEET_LEDGER), 'ID', pen.LinkID);
  if (led && (!led.Note || led.Note === `Modify ${sku}`)) {
    _updateByKey(sheet(SHEET_LEDGER), 'ID', pen.LinkID, { Note: pen.Note });
  }

  // append comment if provided
  const c = (commentOpt && String(commentOpt).trim()) ? String(commentOpt).trim() : '';
  if (c) {
    const user  = me.email || 'unknown';
    const stamp = new Date().toLocaleString();
    const suffix = ` [Approved by ${user} @ ${stamp} — Comment: ${c}]`;

    const penAfter = _findBy(sheet(SHEET_PENDING), 'PendingID', pendingId);
    const newPenNote = (penAfter && penAfter.Note ? penAfter.Note : '') + suffix;
    _updateByKey(sheet(SHEET_PENDING), 'PendingID', pendingId, { Note: newPenNote });

    const ledAfter = _findBy(sheet(SHEET_LEDGER), 'ID', pen.LinkID);
    const newLedNote = (ledAfter && ledAfter.Note ? ledAfter.Note : (pen.Note || '')) + suffix;
    _updateByKey(sheet(SHEET_LEDGER), 'ID', pen.LinkID, { Note: newLedNote });
  }

  // send notifications using the final updated row (so the comment shows up)
  const penFinal = _findBy(sheet(SHEET_PENDING), 'PendingID', pendingId);
  notifyApprovedEvent(type, penFinal);
  notifyRequesterResult('Approved', penFinal);

  return { ok:true };
}

function declinePending(pendingId, reason) {
  const me = getCurrentUser();
  if (!['controller','manager'].includes(me.role) || me.status !== 'Active') {
    throw new Error('Only controllers or managers can decline.');
  }
  if (!reason || !String(reason).trim()) throw new Error('A reason is required to decline.');
  const pen = _findBy(sheet(SHEET_PENDING), 'PendingID', pendingId);
  if (!pen) throw new Error('Request not found.');
  if (String(pen.Status) !== 'Pending') {
    throw new Error('This request has already been processed (status: ' + pen.Status + ').');
  }

  const user  = me.email || 'unknown';
  const stamp = new Date().toLocaleString();

  _updateByKey(sheet(SHEET_PENDING), 'PendingID', pendingId, {
    Status: 'Declined',
    Reason: String(reason).trim()
  });

  const led = _findBy(sheet(SHEET_LEDGER), 'ID', pen.LinkID);
  if (led) {
    const newLedNote = `${led.Note || ''} [Declined by ${user} @ ${stamp} — Reason: ${String(reason).trim()}]`;
    _updateByKey(sheet(SHEET_LEDGER), 'ID', pen.LinkID, { Status: 'Declined', Note: newLedNote });
  }

  const updated = _findBy(sheet(SHEET_PENDING), 'PendingID', pendingId);
  notifyRequesterResult('Declined', updated);
  return { ok:true };
}

/** ===================== Signed Action Links (Approve/Decline in Email) ===================== **/

// Run once from editor to create the secret salt in Script Properties.
function initAuthSalt() {
  var rand = Utilities.getUuid() + ':' + Utilities.getUuid();
  PropertiesService.getScriptProperties().setProperty('AUTH_SALT', rand);
  return 'AUTH_SALT set.';
}
function getAuthSalt_() {
  var salt = PropertiesService.getScriptProperties().getProperty('AUTH_SALT');
  if (!salt) throw new Error('AUTH_SALT not set. Run initAuthSalt() once.');
  return salt;
}
function _b64url_(bytesOrString) {
  // Accept Uint8[] or string
  var bytes = Array.isArray(bytesOrString)
    ? bytesOrString
    : Utilities.newBlob(String(bytesOrString)).getBytes();

  // ❌ return Utilities.base64EncodeWebSafe(bytes, true);
  // ✅ just one argument:
  return Utilities.base64EncodeWebSafe(bytes);
}

function _b64urlToString_(b64) {
  return Utilities.newBlob(Utilities.base64DecodeWebSafe(String(b64))).getDataAsString();
}
function _sign_(payloadString) {
  var key = getAuthSalt_();
  var raw = Utilities.computeHmacSha256Signature(payloadString, key);
  return _b64url_(raw);
}
function _verifyToken_(token) {
  // token format: base64url(payload).base64url(sig)
  var parts = String(token || '').split('.');
  if (parts.length !== 2) throw new Error('Invalid token format');
  var payloadStr = _b64urlToString_(parts[0]);
  var expected   = _sign_(payloadStr);
  if (expected !== parts[1]) throw new Error('Bad signature');
  var data = JSON.parse(payloadStr);
  if (!data || !data.pid || !data.u || !data.a || !data.exp) throw new Error('Malformed token');
  if (Date.now() > Number(data.exp)) throw new Error('Token expired');
  return data; // { a: 'approve'|'decline', pid, u, exp }
}
function makeActionLink_(action, pendingId, recipientEmail, ttlMinutes) {
  var exp = Date.now() + (Math.max(1, ttlMinutes || (3*24*60))) * 60 * 1000; // default 3 days
  var payload = JSON.stringify({ a: action, pid: String(pendingId), u: String(recipientEmail).toLowerCase(), exp: exp });
  var tok = _b64url_(payload) + '.' + _sign_(payload);
  var base = webAppUrl(); // your helper
  return base + '?action=' + encodeURIComponent(action) + '&t=' + encodeURIComponent(tok);
}
function actionButtonsHtml_(buttons) {
  // buttons: [{text, href, bg?, color?}]
  var cells = buttons.map(function(b){
    var bg = b.bg || '#0d6efd', color = b.color || '#fff';
    return '' +
      '<td align="center" style="padding:0 6px 8px">' +
        '<a href="'+b.href+'" ' +
           'style="display:inline-block;padding:10px 16px;border-radius:8px;background:'+bg+';color:'+color+';text-decoration:none;font-weight:700">' +
           b.text +
        '</a>' +
      '</td>';
  }).join('');
  return '' +
    '<table role="presentation" width="100%" style="margin-top:18px"><tr>' +
      '<td align="center">' +
        '<table role="presentation" style="margin:0 auto"><tr>' + cells + '</tr></table>' +
      '</td>' +
    '</tr></table>';
}

/* ---------------- Test data helpers ---------------- */
function seedTestData() {
  const ss = getSS();
  ensureSheets(ss);

  const meEmail = getActiveEmail();
  const usersSh = sheet(SHEET_USERS);
  const users = _readObjects(usersSh);
  const meRow = users.find(u => String(u.Email).toLowerCase() === meEmail.toLowerCase());
  if (meRow && meRow.Status !== 'Active') {
    _updateByKey(usersSh, 'UserID', meRow.UserID, { Status:'Active' });
  } else if (!meRow) {
    const uid = 'USR-' + pad(nextCounter(P_USER), 5);
    _append(usersSh, { UserID:uid, Email:meEmail, Name:'Demo User', Department:'Procurement', RequestedRole:'user', Role:'user', Status:'Active', CreatedAt:nowISO() });
  }

  if (sheet(SHEET_ITEMS).getLastRow() === 1) {
    [
      {SKU:'YDC-PROC-0001', Name:'RJ45 Cat6 Cable 1m', Description:'Ethernet patch cable', UoM:'pc',   Location:'Main WH',   Qty:120, Status:'Active'},
      {SKU:'YDC-PROC-0002', Name:'Logitech B100 Mouse', Description:'USB optical mouse',     UoM:'pc',   Location:'IT Storage', Qty:35,  Status:'Active'},
      {SKU:'YDC-PROC-0003', Name:'A4 Copy Paper',       Description:'80gsm white paper',     UoM:'ream', Location:'Supply RM',  Qty:50,  Status:'Active'}
    ].forEach(it => _append(sheet(SHEET_ITEMS), { ...it, CreatedAt:nowISO(), UpdatedAt:nowISO() }));
  }

  if (sheet(SHEET_LEDGER).getLastRow() === 1) {
    _append(sheet(SHEET_LEDGER), { ID: nextTrxId(), When: nowISO(), Type:'SEED', SKU:'', Item:'Initial seed', Delta:'', UoM:'', Status:'Approved', By: meEmail, Note:'DB initialized' });
  }
  return { ok:true, info:getSpreadsheetInfo() };
}

/* ---------------- HTTP ---------------- */
function doGet(e) {
  // Action router for email clicks
  if (e && e.parameter && e.parameter.action) {
    return handleActionGet_(e);
  }
  // Default: load the main web app
  return HtmlService.createTemplateFromFile('index')
    .evaluate()
    .setTitle(APP_NAME)
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function handleActionGet_(e) {
  try {
    var tok = e.parameter.t || e.parameter.token;
    var act = (e.parameter.action || '').toLowerCase(); // 'approve' | 'decline'
    var data = _verifyToken_(tok); // {a,pid,u,exp}

    // Must be signed in as the intended approver
    var me = getCurrentUser();
    if (!me.email) return renderActionPage_('Sign-in required', 'Please sign in with your YDC account, then click the link again.', 'info');

    if (String(me.email).toLowerCase() !== String(data.u).toLowerCase()) {
      var switchLink = 'https://accounts.google.com/Logout';
      return renderActionPage_(
        'Wrong Google Account',
        'This link was issued to <b>' + data.u + '</b> but you are signed in as <b>' + me.email + '</b>.<br>' +
        'Please <a href="'+switchLink+'" target="_blank" rel="noopener">switch accounts</a> and try again.',
        'warning'
      );
    }

    // Must be controller or manager
    if (!( ['controller','manager'].includes(String(me.role)) && String(me.status) === 'Active')) {
      return renderActionPage_('Not authorized',
        'Only Controllers or Managers with Active status can approve/decline. Your role: ' + (me.role || '—') + '.', 'danger');
    }

    // === NEW: show "already processed" page instead of forms ===
    var pen = _findBy(sheet(SHEET_PENDING), 'PendingID', data.pid);
    if (!pen) {
      return renderActionPage_('Request not found',
        'This request no longer exists (it may have been archived).', 'warning');
    }
    if (String(pen.Status) !== 'Pending') {
      var st = String(pen.Status);
      var kind = st === 'Approved' ? 'success' : (st === 'Declined' ? 'danger' : 'warning');
      return renderActionPage_(
        'Already ' + st,
        'This request has already been <b>' + st.toLowerCase() + '</b>. No further action is possible from this link.',
        kind,
        data.pid
      );
    }

    // Approve: open mini form (optional comment)
    if (act === 'approve') {
      return renderApproveFormPage_(data.pid);
    }

    // Decline: open mini form (required reason)
    if (act === 'decline') {
      return renderDeclineFormPage_(data.pid);
    }

    // Fallback
    return renderActionPage_('Unknown action', 'Unsupported action: ' + act, 'danger');

  } catch (err) {
    return renderActionPage_('Link error', err.message || String(err), 'danger');
  }
}

function renderApproveFormPage_(pendingId) {
  var html = '' +
'<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1">' +
'<style>@keyframes spin{to{transform:rotate(360deg)}}</style>' +
'<div style="max-width:720px;margin:42px auto;padding:22px 24px;border:1px solid #e5e7eb;border-radius:16px;background:#fff;font:14px/1.5 -apple-system,Segoe UI,Roboto,Arial;color:#111">' +
  '<div style="font-weight:800;font-size:22px;margin-bottom:8px;color:#16a34a">Approve Request</div>' +
  '<div style="color:#374151">Pending ID: <b>'+pendingId+'</b></div>' +
  '<label style="display:block;margin-top:12px;font-weight:600">Comment (optional)</label>' +
  '<textarea id="comment" rows="3" placeholder="Add a note for the requester/ledger (optional)" ' +
           'style="width:100%;padding:10px;border:1px solid #d1d5db;border-radius:10px"></textarea>' +
  '<div style="margin-top:14px;text-align:center">' +
    '<button id="btnApprove" style="padding:10px 16px;border-radius:8px;background:#16a34a;color:#fff;border:0;font-weight:700">Submit Approve</button>' +
    '<a href="'+webAppUrl()+'" style="display:inline-block;margin-left:8px;padding:10px 16px;border-radius:8px;background:#0d6efd;color:#fff;text-decoration:none;font-weight:700">Open Web App</a>' +
    '<span id="spin" style="display:none;margin-left:8px;width:14px;height:14px;border:2px solid #d1d5db;border-top-color:#0d6efd;border-radius:50%;display:inline-block;vertical-align:middle;animation:spin .8s linear infinite"></span>' +
  '</div>' +
  '<div id="msg" style="margin-top:12px;color:#374151;text-align:center"></div>' +
'</div>' +
'<script>' +
'  (function(){' +
'    var btn = document.getElementById("btnApprove");' +
'    var msg = document.getElementById("msg");' +
'    var spin = document.getElementById("spin");' +
'    btn.addEventListener("click", function(){' +
'      var c = (document.getElementById("comment").value || "").trim();' +
'      btn.disabled = true; spin.style.display = "inline-block"; msg.textContent = "Submitting approval…";' +
'      google.script.run' +
'        .withSuccessHandler(function(){' +
'          msg.innerHTML = "Approved successfully. You can close this tab.";' +
'          spin.style.display = "none";' +
'        })' +
'        .withFailureHandler(function(e){' +
'          msg.innerHTML = "Error: " + (e && e.message ? e.message : e);' +
'          btn.disabled = false; spin.style.display = "none";' +
'        })' +
'        .approvePending("'+pendingId+'", c);' +   // <— pass optional comment
'    });' +
'  })();' +
'</script>';
  return HtmlService.createHtmlOutput(html)
    .setTitle(APP_NAME + ' — Approve')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function renderActionPage_(title, message, kind, pidOpt) {
  var color = (kind === 'success') ? '#16a34a' :
              (kind === 'warning') ? '#b38700' :
              (kind === 'danger')  ? '#a61b29' : '#0d6efd';
  var back = webAppUrl();
  var body = '' +
    '<div style="max-width:720px;margin:42px auto;padding:22px 24px;border:1px solid #e5e7eb;border-radius:16px;background:#fff;font:14px/1.5 -apple-system,Segoe UI,Roboto,Arial;color:#111">' +
      '<div style="font-weight:800;font-size:22px;margin-bottom:8px;color:'+color+'">'+title+'</div>' +
      '<div style="color:#374151">'+message+'</div>' +
      (pidOpt ? '<div style="margin-top:10px;color:#6b7280">Pending ID: '+pidOpt+'</div>' : '') +
      '<div style="margin-top:16px"><a href="'+back+'" style="display:inline-block;padding:10px 16px;border-radius:8px;background:#0d6efd;color:#fff;text-decoration:none;font-weight:700">Open Web App</a></div>' +
    '</div>';
  return HtmlService.createHtmlOutput('<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1">'+body)
    .setTitle(APP_NAME)
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function renderDeclineFormPage_(pendingId) {
  var html = '' +
'<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1">' +
'<style>@keyframes spin{to{transform:rotate(360deg)}}</style>' +
'<div style="max-width:720px;margin:42px auto;padding:22px 24px;border:1px solid #e5e7eb;border-radius:16px;background:#fff;font:14px/1.5 -apple-system,Segoe UI,Roboto,Arial;color:#111">' +
  '<div style="font-weight:800;font-size:22px;margin-bottom:8px;color:#a61b29">Decline Request</div>' +
  '<div style="color:#374151">Pending ID: <b>'+pendingId+'</b></div>' +
  '<label style="display:block;margin-top:12px;font-weight:600">Reason (required)</label>' +
  '<textarea id="reason" rows="3" style="width:100%;padding:10px;border:1px solid #d1d5db;border-radius:10px"></textarea>' +
  '<div style="margin-top:14px;text-align:center">' +
    '<button id="btnGo" style="padding:10px 16px;border-radius:8px;background:#a61b29;color:#fff;border:0;font-weight:700">Submit Decline</button>' +
    '<a href="'+webAppUrl()+'" style="display:inline-block;margin-left:8px;padding:10px 16px;border-radius:8px;background:#0d6efd;color:#fff;text-decoration:none;font-weight:700">Open Web App</a>' +
    '<span id="spin" style="display:none;margin-left:8px;width:14px;height:14px;border:2px solid #d1d5db;border-top-color:#0d6efd;border-radius:50%;display:inline-block;vertical-align:middle;animation:spin .8s linear infinite"></span>' +
  '</div>' +
  '<div id="msg" style="margin-top:12px;color:#374151;text-align:center"></div>' +
'</div>' +
'<script>' +
'  (function(){' +
'    var btn = document.getElementById("btnGo");' +
'    var msg = document.getElementById("msg");' +
'    var spin = document.getElementById("spin");' +
'    btn.addEventListener("click", function(){' +
'      var r = (document.getElementById("reason").value || "").trim();' +
'      if(!r){ alert("Please enter a reason."); return; }' +
'      btn.disabled = true; spin.style.display = "inline-block"; msg.textContent = "Submitting decline…";' +
'      google.script.run' +
'        .withSuccessHandler(function(){' +
'          msg.innerHTML = "Declined successfully. You can close this tab.";' +
'          spin.style.display = "none";' +
'        })' +
'        .withFailureHandler(function(e){' +
'          msg.innerHTML = "Error: " + (e && e.message ? e.message : e);' +
'          btn.disabled = false; spin.style.display = "none";' +
'        })' +
'        .declinePending("'+pendingId+'", r);' +
'    });' +
'  })();' +
'</script>';
  return HtmlService.createHtmlOutput(html)
    .setTitle(APP_NAME + ' — Decline')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

/* ---------------- Void (controller) ---------------- */
function voidPending(pendingId, reason) {
  const me = getCurrentUser();
  if (!['controller','manager'].includes(me.role) || me.status !== 'Active') {
    throw new Error('Only controllers or managers can void.');
  }
  if (!reason || !String(reason).trim()) throw new Error('A reason is required to void.');

  const pen = _findBy(sheet(SHEET_PENDING), 'PendingID', pendingId);
  if (!pen || pen.Status !== 'Pending') throw new Error('Pending record not found');

  const user  = me.email || 'unknown';
  const stamp = new Date().toLocaleString();

  const newPenNote = `${pen.Note || ''} [Voided by ${user} @ ${stamp}]`;
  _updateByKey(sheet(SHEET_PENDING), 'PendingID', pendingId, {
    Status: 'Voided',
    Note: newPenNote,
    Reason: String(reason).trim()
  });

  const led = _findBy(sheet(SHEET_LEDGER), 'ID', pen.LinkID);
  if (led) {
    const newLedNote = `${led.Note || ''} [Voided by ${user} @ ${stamp} — Reason: ${String(reason).trim()}]`;
    _updateByKey(sheet(SHEET_LEDGER), 'ID', pen.LinkID, { Status: 'Voided', Note: newLedNote });
  }

  const updated = _findBy(sheet(SHEET_PENDING), 'PendingID', pendingId);
  notifyRequesterResult('Voided', updated);
  return { ok: true };
}

/* =======================================================================
   ========================== NOTIFICATIONS ==============================
   ======================================================================= */

// --- Routing helpers ---
function readNotifyConfig() {
  const rows = _readObjects(sheet(SHEET_NOTIFY));
  const map = {};
  rows.forEach(r => {
    const key = String(r.Event || '').trim();
    if (!key) return;
    map[key] = {
      enabled: String(r.Enabled).toLowerCase() !== 'false' && String(r.Enabled).toLowerCase() !== '0',
      recipients: parseEmailList(r.Recipients || ''),
      cc: parseEmailList(r.CC || ''),
      threshold: (r.Threshold === '' || r.Threshold === null) ? null : Number(r.Threshold),
      hour: (r.Hour === '' || r.Hour === null) ? null : Number(r.Hour),
      note: r.Note || ''
    };
  });
  return map;
}
function parseEmailList(s) {
  return String(s || '')
    .split(/[,;\n]/)
    .map(x => x.trim())
    .filter(x => x && /@/.test(x))
    .filter((v,i,a)=> a.indexOf(v)===i);
}
function getApprovers() {
  return _readObjects(sheet(SHEET_USERS))
    .filter(u => ['controller','manager'].includes(String(u.Role)) && String(u.Status) === 'Active')
    .map(u => String(u.Email || '').trim())
    .filter(Boolean);
}

function getControllers() {
  return _readObjects(sheet(SHEET_USERS))
    .filter(u => String(u.Role) === 'controller' && String(u.Status) === 'Active')
    .map(u => String(u.Email || '').trim())
    .filter(Boolean);
}
function resolveRecipients(eventKey, opts) {
  // When controllersOnly is set, we actually want "approvers only"
  if (opts && opts.controllersOnly) {
    return { to: getApprovers(), cc: [] };
  }
  const cfg = readNotifyConfig()[eventKey] || { enabled: true, recipients: [], cc: [] };
  if (!cfg.enabled) return { to: [], cc: [] };
  let to = [].concat(cfg.recipients || []);
  if (opts && opts.includeControllers) to = to.concat(getControllers());
  to = to.filter(Boolean).filter((v,i,a)=> a.indexOf(v)===i);
  return { to, cc: cfg.cc || [] };
}
function sendMailSafe(to, subject, html, cc) {
  if (!to || to.length === 0) return;
  try {
    MailApp.sendEmail({
      to: to.join(','),
      subject,
      htmlBody: html,
      cc: (cc && cc.length) ? cc.join(',') : '',
      name: APP_NAME,
      noReply: true
    });
  } catch(e) { Logger.log('sendMail failed: ' + e); }
}
function safe(v){ return (v===null || v===undefined || v==='') ? '—' : String(v); }
function webAppUrl(){
  // If not deployed yet, this may be blank. (We still render the button.)
  return ScriptApp.getService().getUrl() || '';
}
function friendlyType(t){
  return ({
    CREATE_SKU:'Create SKU',
    MODIFY_SKU:'Modify Item',
    RETIRE_SKU:'Retire SKU',
    RECEIVE:'Receive (Inbound)',
    ISSUE:'Issue (Outbound)',
    USER_CREATED:'New User',
    PENDING:'Pending'
  })[t] || t;
}

/* ---------- Email UI (centered card) ---------- */
function cardEmail(title, rows, opts) {
  var btnText = (opts && opts.ctaText) || 'Open Web App';
  var btnHref = (opts && opts.ctaHref) || webAppUrl();
  var subtitle = (opts && opts.subtitle)
    ? '<div style="color:#6b7280;text-align:center;margin:6px 0 12px">'+opts.subtitle+'</div>' : '';

  var actionsHtml = (opts && opts.buttons && opts.buttons.length)
    ? actionButtonsHtml_(opts.buttons)
    : '<div style="text-align:center;margin-top:18px">' +
        '<a href="'+btnHref+'" style="display:inline-block;padding:10px 16px;border-radius:8px;background:#0d6efd;color:#fff;text-decoration:none;font-weight:700">'+btnText+'</a>' +
      '</div>';

  var tableRows = rows.map(function(pair){
    return '<tr>' +
      '<td style="width:220px;padding:10px 12px;border:1px solid #e5e7eb;background:#f9fafb;color:#6b7280">'+pair[0]+'</td>' +
      '<td style="padding:10px 12px;border:1px solid #e5e7eb">'+pair[1]+'</td>' +
    '</tr>';
  }).join('');

  return '' +
  '<center>' +
    '<table role="presentation" width="100%" style="background:#f6f7fb;padding:24px 0">' +
      '<tr><td>' +
        '<table role="presentation" width="680" align="center" style="margin:0 auto;border-collapse:separate;border-spacing:0 14px">' +
          '<tr><td align="center" style="font:700 14px -apple-system,Segoe UI,Roboto,Arial;color:#111">'+APP_NAME+'</td></tr>' +
          '<tr><td>' +
            '<table role="presentation" width="680" align="center" style="margin:0 auto;background:#ffffff;border:1px solid #e5e7eb;border-radius:14px">' +
              '<tr><td style="padding:24px 28px">' +
                '<div style="font:700 22px -apple-system,Segoe UI,Roboto,Arial;color:#111;text-align:center">'+title+'</div>' +
                subtitle +
                '<table role="presentation" width="100%" style="border-collapse:collapse;margin-top:6px">'+tableRows+'</table>' +
                actionsHtml +
              '</td></tr>' +
            '</table>' +
          '</td></tr>' +
        '</table>' +
      '</td></tr>' +
    '</table>' +
  '</center>';
}


/* ---------- Per-event notifiers ---------- */

// Approval queued: controllers only + requester gets a separate email
function notifyPendingCreated(ctx){
  var r = ctx.rec;
  var typeNice = friendlyType(r.type);

  // 1) Controllers / approvers — send personalized email so each has their own signed link
  var recips = resolveRecipients(NE.PENDING, { controllersOnly:true }).to;
  if (recips.length){
    recips.forEach(function(toEmail){
      var approveUrl = makeActionLink_('approve', ctx.pendingId, toEmail);
      var declineUrl = makeActionLink_('decline', ctx.pendingId, toEmail);

      var html = cardEmail(
        'Approval Needed — ' + typeNice,
        [
          ['Type', typeNice],
          ['SKU', safe(r.sku)],
          ['Item', safe(r.name)],
          ['Quantity Δ', safe(r.delta)],
          ['Requested By', safe(ctx.by)],
          ['Note', safe(r.note)],
          ['Pending ID', safe(ctx.pendingId)],
          ['Ledger ID', safe(ctx.linkId)]
        ],
        {
          subtitle: 'You can approve directly from this email.',
          buttons: [
            { text:'Approve', href: approveUrl, bg:'#16a34a' },
            { text:'Decline', href: declineUrl, bg:'#dc2626' },
            { text:'Open Web App', href: webAppUrl(), bg:'#0d6efd' }
          ]
        }
      );
      sendMailSafe([toEmail], ('[Approval Needed] ' + typeNice + ' ' + (r.sku || '')).trim(), html, []);
    });
  }

  // 2) Requester copy: “submitted for approval”
  if (ctx.by && /@/.test(ctx.by)) {
    var html2 = cardEmail(
      'Submitted for Approval — ' + typeNice,
      [
        ['Status', 'Pending'],
        ['Type', typeNice],
        ['SKU', safe(r.sku)],
        ['Item', safe(r.name)],
        ['Quantity Δ', safe(r.delta)],
        ['Note', safe(r.note)],
        ['Pending ID', safe(ctx.pendingId)]
      ],
      { subtitle:'Your request has been queued and is awaiting controller approval.' }
    );
    sendMailSafe([ctx.by], ('[Submitted] ' + typeNice + ' ' + (r.sku || '')).trim(), html2, []);
  }
}

// Approved event (goes to configured recipients for that type)
function notifyApprovedEvent(type, penRow){
  const key = ({
    RECEIVE:NE.RECEIVE,
    ISSUE:NE.ISSUE,
    CREATE_SKU:NE.CREATE_SKU,
    MODIFY_SKU:NE.MODIFY_SKU,
    RETIRE_SKU:NE.RETIRE_SKU
  })[type];
  if (!key) return;
  const { to, cc } = resolveRecipients(key, {});
  if (!to.length) return;

  const typeNice = friendlyType(type);
  const html = cardEmail(
    `Approved — ${typeNice}`,
    [
      ['SKU', safe(penRow.SKU)],
      ['Item', safe(penRow.Name)],
      ['UoM', safe(penRow.UoM)],
      ['Quantity', safe(penRow.Qty)],
      ['Δ', safe(penRow.Delta)],
      ['Requested By', safe(penRow.By)],
      ['Note', safe(penRow.Note)],
      ['Pending ID', safe(penRow.PendingID)],
      ['Ledger ID', safe(penRow.LinkID)]
    ],
    { subtitle:'This request has been approved.' }
  );
  sendMailSafe(to, `[Approved] ${typeNice} ${penRow.SKU || ''}`.trim(), html, cc);
}

// Requester result (Approved/Declined)
function notifyRequesterResult(result, penRow){
  const to = (penRow.By && /@/.test(penRow.By)) ? [penRow.By] : [];
  if (!to.length) return;

  const typeNice = friendlyType(penRow.Type);
  const subtitle =
    result === 'Approved' ? 'Your request has been approved.' :
    result === 'Declined' ? 'Your request has been declined.' :
    result === 'Voided'   ? 'Your request was voided (removed from the queue) by a controller.' :
    '';

  const rows = [
    ['Type', typeNice],
    ['SKU', safe(penRow.SKU)],
    ['Item', safe(penRow.Name)],
    ['UoM', safe(penRow.UoM)],
    ['Quantity', safe(penRow.Qty)],
    ['Δ', safe(penRow.Delta)],
    ['Note', safe(penRow.Note)],
    // NEW: show reason if present (esp. Declined/Voided)
    ['Reason', safe(penRow.Reason)],
    ['Pending ID', safe(penRow.PendingID)],
    ['Ledger ID', safe(penRow.LinkID)]
  ];

  const html = cardEmail(`${result} — ${typeNice}`, rows, { subtitle });
  sendMailSafe(to, `[${result}] ${typeNice} ${penRow.SKU || ''}`.trim(), html, []);
}



function notifyUserCreated(u){
  const { to, cc } = resolveRecipients(NE.USER_CREATED, { includeControllers:true });
  if (!to.length) return;
  const html = cardEmail(
    'New User Request',
    [
      ['Email', safe(u.email)],
      ['Name', safe(u.name)],
      ['Department', safe(u.department)],
      ['Requested Role', safe(u.requestedRole || 'user')]
    ],
    { subtitle:'A new user is requesting access.' }
  );
  sendMailSafe(to, `[New User] ${u.name || u.email}`, html, cc);
}

/* ---------- Low Stock ---------- */
function getLowStockThreshold() {
  const cfg = readNotifyConfig()[NE.LOW_STOCK];
  const thr = cfg && typeof cfg.threshold === 'number' && !isNaN(cfg.threshold) ? cfg.threshold : 5;
  return Math.max(0, thr);
}
function maybeNotifyLowStock(itemsSubset) {
  const threshold = getLowStockThreshold();
  const low = (itemsSubset || []).filter(x =>
    String(x.Status) === 'Active' && Number(x.Qty || 0) <= threshold
  );
  if (!low.length) return;

  const { to, cc } = resolveRecipients(NE.LOW_STOCK, { includeControllers:true });
  if (!to.length) return;

  const rows = low.map(it => [
    'SKU', `${safe(it.SKU)}`
  ]).flat(); // not used; we want a table of many items, so do a mini table below

  const lowRowsTable = `
    <table role="presentation" width="100%" style="border-collapse:collapse;margin-top:6px">
      <thead>
        <tr>
          <th style="padding:8px 10px;border:1px solid #e5e7eb;background:#f9fafb;text-align:left;color:#6b7280">SKU</th>
          <th style="padding:8px 10px;border:1px solid #e5e7eb;background:#f9fafb;text-align:left;color:#6b7280">Item</th>
          <th style="padding:8px 10px;border:1px solid #e5e7eb;background:#f9fafb;text-align:right;color:#6b7280">Qty</th>
          <th style="padding:8px 10px;border:1px solid #e5e7eb;background:#f9fafb;text-align:left;color:#6b7280">UoM</th>
          <th style="padding:8px 10px;border:1px solid #e5e7eb;background:#f9fafb;text-align:left;color:#6b7280">Location</th>
        </tr>
      </thead>
      <tbody>
        ${low.map(it => `
          <tr>
            <td style="padding:8px 10px;border:1px solid #e5e7eb">${safe(it.SKU)}</td>
            <td style="padding:8px 10px;border:1px solid #e5e7eb">${safe(it.Name)}</td>
            <td style="padding:8px 10px;border:1px solid #e5e7eb;text-align:right"><b>${safe(it.Qty)}</b></td>
            <td style="padding:8px 10px;border:1px solid #e5e7eb">${safe(it.UoM)}</td>
            <td style="padding:8px 10px;border:1px solid #e5e7eb">${safe(it.Location)}</td>
          </tr>`).join('')}
      </tbody>
    </table>`;

  const html = cardEmail(
    `Low Stock Alert (≤ ${threshold}) — ${low.length} item(s)`,
    [['Notice', 'Items at or below the minimum threshold.']],
    { subtitle:'Review items that need replenishment.' }
  ).replace('</table></td></tr></table></td></tr></table></center>', // inject our list before closing
     lowRowsTable + '</table></td></tr></table></td></tr></table></center>');

  sendMailSafe(to, `Low Stock Alert (≤ ${threshold}) — ${low.length} item(s)`, html, cc);
}
function scanLowStockAndNotify() {
  const items = getItems().filter(x => String(x.Status) === 'Active');
  maybeNotifyLowStock(items);
}

/* ---------- Daily KPI ---------- */
function renderKpiEmail() {
  const c = getCounts();
  const items = getItems();
  const threshold = getLowStockThreshold();
  const low = items.filter(x => String(x.Status) === 'Active' && Number(x.Qty||0) <= threshold);

  const rows = [
    ['Active SKUs', String(c.activeSkus)],
    ['Total On-hand', String(c.onhand)],
    ['Pending Approvals', String(c.pending)],
    ['Ledger Records', String(c.ledger)]
  ];
  const main = cardEmail('Daily KPI', rows, { subtitle:'Snapshot of today’s key metrics.' });

  const lowRowsTable = `
    <div style="margin-top:10px;font-weight:600">Low Stock (≤ ${threshold}) — ${low.length} item(s)</div>
    <table role="presentation" width="100%" style="border-collapse:collapse;margin-top:6px">
      <thead>
        <tr>
          <th style="padding:8px 10px;border:1px solid #e5e7eb;background:#f9fafb;text-align:left;color:#6b7280">SKU</th>
          <th style="padding:8px 10px;border:1px solid #e5e7eb;background:#f9fafb;text-align:left;color:#6b7280">Item</th>
          <th style="padding:8px 10px;border:1px solid #e5e7eb;background:#f9fafb;text-align:right;color:#6b7280">Qty</th>
          <th style="padding:8px 10px;border:1px solid #e5e7eb;background:#f9fafb;text-align:left;color:#6b7280">UoM</th>
          <th style="padding:8px 10px;border:1px solid #e5e7eb;background:#f9fafb;text-align:left;color:#6b7280">Location</th>
        </tr>
      </thead>
      <tbody>
        ${low.map(it => `
          <tr>
            <td style="padding:8px 10px;border:1px solid #e5e7eb">${safe(it.SKU)}</td>
            <td style="padding:8px 10px;border:1px solid #e5e7eb">${safe(it.Name)}</td>
            <td style="padding:8px 10px;border:1px solid #e5e7eb;text-align:right"><b>${safe(it.Qty)}</b></td>
            <td style="padding:8px 10px;border:1px solid #e5e7eb">${safe(it.UoM)}</td>
            <td style="padding:8px 10px;border:1px solid #e5e7eb">${safe(it.Location)}</td>
          </tr>`).join('')}
      </tbody>
    </table>`;
  // Attach the low stock table just before closure
  return main.replace('</table></td></tr></table></td></tr></table></center>',
    lowRowsTable + '</table></td></tr></table></td></tr></table></center>');
}
function sendDailyKPI() {
  const { to, cc } = resolveRecipients(NE.DAILY_KPI, {});
  if (!to.length) return;
  const html = renderKpiEmail();
  sendMailSafe(to, 'YDC — Daily KPI', html, cc);
}

/* ---------- Installable Triggers ---------- */
function installDailyTriggerFromSheet() {
  const fn = 'dailyTick';
  ScriptApp.getProjectTriggers().forEach(t => { if (t.getHandlerFunction() === fn) ScriptApp.deleteTrigger(t); });
  const cfg = readNotifyConfig()[NE.DAILY_KPI];
  const hour = (cfg && typeof cfg.hour === 'number' && !isNaN(cfg.hour)) ? Math.min(23, Math.max(0, cfg.hour)) : 8;
  ScriptApp.newTrigger(fn).timeBased().atHour(hour).everyDays(1).create();
  return { ok:true, message:`Daily trigger installed at hour ${hour}` };
}
function dailyTick(){
  sendDailyKPI();
  scanLowStockAndNotify();
}

/* =======================================================================
   ========== BACKUP + ARCHIVE (Configured to your IDs) ===================
   ======================================================================= */

/**
 * You asked to:
 *  - Save backups into a specific folder
 *  - Archive by **cut & transfer** into a separate spreadsheet that already has only Pending & Ledger
 *  - No auto-trigger installers (you will add triggers yourself)
 */

// === Your fixed locations ===
const DEFAULT_BACKUP_FOLDER_ID = '1Uq4NEDwbOE0EtSjxIhjYp85B9RxUYtcY';
const DEFAULT_ARCHIVE_SSID     = '1DACB0EeTGnXyMm1F6cRu3XfyZS4kBlVlZ88Nh8_K3c4';

// Allow overrides via Script Properties (optional)
const PROP_BACKUP_FOLDER_ID = 'YDC_PROC_BACKUP_FOLDER_ID';
const PROP_ARCHIVE_SSID     = 'YDC_PROC_ARCHIVE_SSID';

// Parse an ID from a URL or raw ID
function _parseId(s){ const m = String(s||'').trim().match(/[-\w]{25,}/); if(!m) throw new Error('Supply a valid Drive ID or URL'); return m[0]; }

// --- (Optional) setters if you ever change locations ---
function setBackupFolderId(idOrUrl){
  const id = _parseId(idOrUrl);
  // Validate access
  DriveApp.getFolderById(id);
  SCRIPTPROP.setProperty(PROP_BACKUP_FOLDER_ID, id);
  return { ok:true, id };
}
function setArchiveSpreadsheetId(idOrUrl){
  const id = _parseId(idOrUrl);
  const ss = SpreadsheetApp.openById(id);
  // validate sheets exist
  if (!ss.getSheetByName(SHEET_LEDGER) || !ss.getSheetByName(SHEET_PENDING)) {
    throw new Error('Archive spreadsheet must contain sheets named "Ledger" and "Pending".');
  }
  SCRIPTPROP.setProperty(PROP_ARCHIVE_SSID, id);
  return { ok:true, id, url:ss.getUrl(), name:ss.getName() };
}

// --- helpers to open the configured targets ---
function ensureUsersTimestampColumnAndBackfill(){
  const sh = sheet(SHEET_USERS);
  // Make sure our canonical columns exist (adds any missing ones at the end)
  ensureColumns(sh, ['UserID','Email','Name','Department','RequestedRole','Role','Status','CreatedAt']);

  // Backfill missing CreatedAt
  const lastRow = sh.getLastRow();
  if (lastRow <= 1) return; // header only

  const hdr = sh.getRange(1,1,1,sh.getLastColumn()).getValues()[0];
  const createdCol = hdr.indexOf('CreatedAt') + 1;
  if (createdCol <= 0) return;

  const range = sh.getRange(2, createdCol, lastRow - 1, 1);
  const vals  = range.getValues();
  let dirty = false;
  for (let i=0; i<vals.length; i++){
    if (!vals[i][0]) {            // only fill blanks
      vals[i][0] = nowISO();      // use ISO so the UI can format it consistently
      dirty = true;
    }
  }
  if (dirty) range.setValues(vals);
}

function getBackupFolder(){
  const id = SCRIPTPROP.getProperty(PROP_BACKUP_FOLDER_ID) || DEFAULT_BACKUP_FOLDER_ID;
  return DriveApp.getFolderById(id);
}
function getArchiveSS(){
  const id = SCRIPTPROP.getProperty(PROP_ARCHIVE_SSID) || DEFAULT_ARCHIVE_SSID;
  const ss = SpreadsheetApp.openById(id);
  // Hard requirement: same sheet names
  if (!ss.getSheetByName(SHEET_LEDGER) || !ss.getSheetByName(SHEET_PENDING)) {
    throw new Error('Archive spreadsheet must contain "Ledger" and "Pending" sheets (exact names).');
  }
  return ss;
}

// --- Backup: copy live spreadsheet into your fixed folder, prune old copies ---
function backupSpreadsheetCopy(keepLast){
  const keep = Math.max(1, keepLast || 14);
  const liveFile = DriveApp.getFileById(getSS().getId());
  const folder = getBackupFolder();

  const ts = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd HHmm');
  const name = `${liveFile.getName()} — Backup ${ts}`;
  const copy = liveFile.makeCopy(name, folder);

  // prune older copies for this live sheet name in that folder
  const files = [];
  const it = folder.getFiles();
  while (it.hasNext()){
    const f = it.next();
    if (f.getName().startsWith(liveFile.getName() + ' — Backup ')){
      files.push(f);
    }
  }
  files.sort((a,b)=> b.getDateCreated() - a.getDateCreated());
  let removed = 0;
  files.slice(keep).forEach(f => { f.setTrashed(true); removed++; });

  return { ok:true, backupId:copy.getId(), backupUrl:copy.getUrl(), pruned:removed, kept:keep };
}

// --- Archive: CUT rows from live to the archive workbook ---
// Criteria (defaults):
//   - Ledger: Status != "Pending" AND When < today - olderThanDays (default 90)
//             PLUS: ensure live ledger capped to keepLatestRows (default 8000) by moving oldest non-pending
//   - Pending: Status != "Pending" AND When < today - olderThanDays
function archiveResolved(opts){
  const olderThanDays  = (opts && opts.olderThanDays)  || 90;
  const keepLatestRows = (opts && opts.keepLatestRows) || 8000;

  const liveSS = getSS();
  const liveLed = liveSS.getSheetByName(SHEET_LEDGER);
  const livePen = liveSS.getSheetByName(SHEET_PENDING);
  if (!liveLed || !livePen) throw new Error('Live sheets "Ledger" and "Pending" are required.');

  const arcSS  = getArchiveSS();
  const arcLed = arcSS.getSheetByName(SHEET_LEDGER);
  const arcPen = arcSS.getSheetByName(SHEET_PENDING);

  const tz = Session.getScriptTimeZone();
  const cutoff = new Date(new Date().getTime() - (olderThanDays * 24 * 60 * 60 * 1000));

  // --- utility: map src row by header names into dest header order (if columns differ)
  function mapByHeaders(srcHdr, destHdr, row){
    const srcIndex = Object.fromEntries(srcHdr.map((h,i)=>[h,i]));
    return destHdr.map(h => (srcIndex[h] != null) ? row[srcIndex[h]] : '');
  }
  function getHeader(sh){ return sh.getRange(1,1,1,Math.max(1,sh.getLastColumn())).getValues()[0]; }

  // ========== LEDGER ==========
  let movedLed = 0;
  if (liveLed.getLastRow() > 1){
    const ledHdr = getHeader(liveLed);
    const idCol   = ledHdr.indexOf('ID');
    const whenCol = ledHdr.indexOf('When');
    const statCol = ledHdr.indexOf('Status');
    if (idCol < 0 || whenCol < 0 || statCol < 0) throw new Error('Ledger must have columns ID, When, Status');

    const nRows = liveLed.getLastRow() - 1;
    const nCols = liveLed.getLastColumn();
    const vals  = liveLed.getRange(2,1,nRows,nCols).getValues();

    // Determine rows older than cutoff and resolved
    const resolvedOldIdx = vals
      .map((r,i)=>({i, when: new Date(r[whenCol]), status:String(r[statCol])}))
      .filter(o => o.status !== 'Pending' && o.when instanceof Date && !isNaN(o.when) && o.when < cutoff)
      .map(o => o.i);

    // Cap by keepLatestRows (move oldest non-pending to hit cap)
    const totalRows = vals.length;
    const needByCap = Math.max(0, totalRows - keepLatestRows);
    let capIdx = [];
    if (needByCap > 0){
      capIdx = vals
        .map((r,i)=>({i, when:new Date(r[whenCol]), status:String(r[statCol])}))
        .filter(o => o.status !== 'Pending' && o.when instanceof Date && !isNaN(o.when))
        .sort((a,b)=> a.when - b.when)
        .slice(0, needByCap)
        .map(o => o.i);
    }

    const toMoveSet = new Set([...resolvedOldIdx, ...capIdx]);
    const toMove = Array.from(toMoveSet).sort((a,b)=>a-b);
    if (toMove.length){
      const destHdr = getHeader(arcLed);
      const rowsToAppend = toMove.map(i => mapByHeaders(ledHdr, destHdr, vals[i]));

      // Append to archive
      arcLed.getRange(arcLed.getLastRow()+1, 1, rowsToAppend.length, destHdr.length).setValues(rowsToAppend);

      // Delete from live (descending row numbers to keep indices stable)
      const rowsSheetIdxDesc = toMove.map(i => i + 2).sort((a,b)=> b - a);
      rowsSheetIdxDesc.forEach(rn => liveLed.deleteRow(rn));

      movedLed = toMove.length;
    }
  }

  // ========== PENDING ==========
  let movedPen = 0;
  if (livePen.getLastRow() > 1){
    const penHdr = getHeader(livePen);
    const pidCol  = penHdr.indexOf('PendingID');
    const whenCol = penHdr.indexOf('When');
    const statCol = penHdr.indexOf('Status');
    if (pidCol < 0 || whenCol < 0 || statCol < 0) throw new Error('Pending must have columns PendingID, When, Status');

    const nRows = livePen.getLastRow() - 1;
    const nCols = livePen.getLastColumn();
    const vals  = livePen.getRange(2,1,nRows,nCols).getValues();

    const toMove = vals
      .map((r,i)=>({i, when:new Date(r[whenCol]), status:String(r[statCol])}))
      .filter(o => o.status !== 'Pending' && (o.when instanceof Date && !isNaN(o.when) ? o.when < cutoff : true))
      .map(o => o.i)
      .sort((a,b)=>a-b);

    if (toMove.length){
      const destHdr = getHeader(arcPen);
      const rowsToAppend = toMove.map(i => mapByHeaders(penHdr, destHdr, vals[i]));

      arcPen.getRange(arcPen.getLastRow()+1, 1, rowsToAppend.length, destHdr.length).setValues(rowsToAppend);

      const rowsSheetIdxDesc = toMove.map(i => i + 2).sort((a,b)=> b - a);
      rowsSheetIdxDesc.forEach(rn => livePen.deleteRow(rn));

      movedPen = toMove.length;
    }
  }

  return {
    ok:true,
    moved: { ledger: movedLed, pending: movedPen },
    cutoffISO: cutoff.toISOString(),
    archiveUrl: arcSS.getUrl()
  };
}

// Optional: a single function you can point a time-based trigger at
function nightlyMaintenance(){
  const backup = backupSpreadsheetCopy(14);                 // keep last 14 copies
  const arch   = archiveResolved({ olderThanDays:90, keepLatestRows:8000 });
  return { ok:true, backup, arch };
}

// Force Users sheet to canonical order and preserve data by header name.
// Final: A..H = UserID, Email, Name, Department, RequestedRole, Role, Status, CreatedAt
function normalizeUsersSheet(){
  const sh = sheet(SHEET_USERS);
  const WANT = ['UserID','Email','Name','Department','RequestedRole','Role','Status','CreatedAt'];

  const lastRow = Math.max(1, sh.getLastRow());
  const lastCol = Math.max(1, sh.getLastColumn());
  const vals = sh.getRange(1, 1, lastRow, lastCol).getValues();

  if (vals.length === 0) { sh.appendRow(WANT); return; }

  const hdrIn  = (vals[0] || []).map(h => String(h||'').trim());
  const rowsIn = vals.slice(1);
  const idx = Object.fromEntries(hdrIn.map((h,i)=>[h,i]));

  const out = [WANT];
  rowsIn.forEach(r => out.push(WANT.map(h => (idx[h] != null) ? r[idx[h]] : '')));

  // Rewrite in correct order
  if (sh.getMaxColumns() < WANT.length) {
    sh.insertColumnsAfter(sh.getMaxColumns(), WANT.length - sh.getMaxColumns());
  }
  sh.clear(); // contents + formatting
  sh.getRange(1, 1, out.length, WANT.length).setValues(out);

  // Trim extras to the right (if any)
  const extra = sh.getLastColumn() - WANT.length;
  if (extra > 0) sh.deleteColumns(WANT.length + 1, extra);
}
