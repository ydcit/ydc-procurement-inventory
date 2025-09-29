
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
const SHEET_BUS_UNITS = 'Business Unit';
const SHEET_DEPTS     = 'Departments';
const SHEET_DEPLOY_LOCS = 'Deployment Location';
const SHEET_PRICES    = 'Prices';
const SHEET_CATEGORIES = 'Category';


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
    [SHEET_USERS]:        ['UserID','Email','Name','Department','RequestedRole','Role','Status','CreatedAt'],
    [SHEET_ITEMS]:        ['SKU','Name','Description','Category','UoM','Location','Qty','Status','CreatedAt','UpdatedAt','UnitPrice'],
    [SHEET_PENDING]:      ['PendingID','LinkID','When','Type','SKU','Details','Name','UoM','Qty','Delta','Reason','Note','By','Status','ReviewedAt','ReviewedBy','PayloadJSON','Stage','NextRole','ApprovalsJSON'], 
    [SHEET_LEDGER]:       ['ID','When','Type','SKU','Item','Delta','UoM','Status','By','ReviewedAt','ReviewedBy','Note','ApprovalsJSON'],    
    [SHEET_NOTIFY]:       ['Event','Enabled','Recipients','CC','Threshold','Hour','Note'],
    [SHEET_BUS_UNITS]:    ['Name','Active'],
    [SHEET_DEPTS]:        ['Name','Active'],
    [SHEET_DEPLOY_LOCS]:  ['Name','Active'], 
    [SHEET_PRICES]:       ['SKU','Price','At','By','Source','Note','LinkID','PendingID'],
    [SHEET_CATEGORIES]:   ['Name','Active']
  };
  Object.keys(want).forEach(name => {
    const sh = ss.getSheetByName(name) || ss.insertSheet(name);
    ensureColumns(sh, want[name]);
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
function getLiveUserData() {
  // Minimal payload for periodic refresh (fast + cheap)
  return {
    user: getCurrentUser(),
    counts: getCounts(),
    pending: getPending(),
    minePending: getMyActivity().minePending,
    serverNow: nowISO(),
    cacheBuster: Utilities.getUuid()
  };
}

function _asDate_(v){
  const d = new Date(v);
  return isNaN(d) ? null : d;
}
function _reqKeyFromRow_(r, isPending){
  // Prefer request-scoped ids; fall back sanely
  return String(
    isPending
      ? (r.PendingID || r.LinkID || r.ID || r.Id)
      : (r.ID       || r.LinkID || r.PendingID || r.Id)
  ).trim();
}
function computeMySLA_(emailLc){
  emailLc = String(emailLc||'').toLowerCase();
  const penAll = _readObjects(sheet(SHEET_PENDING))
                  .filter(r => String(r.By||'').toLowerCase() === emailLc);
  const ledAll = _readObjects(sheet(SHEET_LEDGER))
                  .filter(r => String(r.By||'').toLowerCase() === emailLc);

  const now = new Date();
  const cutoff = new Date(now.getTime() - 30*24*60*60*1000);

  const openKeys = new Set(
    penAll
      .filter(p => String(p.Status) === 'Pending')
      .map(p => _reqKeyFromRow_(p, true))
  );

  const approvedKeys30 = new Set(
    ledAll
      .filter(l => String(l.Status) === 'Approved')
      .filter(l => {
        // prefer ReviewedAt, fallback to When
        const d = _asDate_(l.ReviewedAt || l.When);
        return d && d >= cutoff;
      })
      .map(l => _reqKeyFromRow_(l, false))
  );

  const declinedKeys30 = new Set([
    // Declined finalized into Ledger
    ...ledAll
      .filter(l => String(l.Status) === 'Declined')
      .filter(l => {
        const d = _asDate_(l.ReviewedAt || l.When);
        return d && d >= cutoff;
      })
      .map(l => _reqKeyFromRow_(l, false)),
    // Declined that never left Pending
    ...penAll
      .filter(p => String(p.Status) === 'Declined')
      .filter(p => {
        const d = _asDate_(p.ReviewedAt || p.When);
        return d && d >= cutoff;
      })
      .map(p => _reqKeyFromRow_(p, true))
  ]);

  const totalKeys = new Set([
    ...penAll.map(p => _reqKeyFromRow_(p, true)),
    ...ledAll.map(l => _reqKeyFromRow_(l, false))
  ]);

  return {
    open: openKeys.size,
    approved30: approvedKeys30.size,
    declined30: declinedKeys30.size,
    total: totalKeys.size
  };
}

/**
 * Return a normalized approval flow for a pending transaction.
 * Output shape (array in intended order):
 *   [{ role, name, email, status, at, isCurrent }]
 *
 * How it works (robust/fallback-friendly):
 *  - If Pending row has PayloadJSON.approval.flow, normalize & return it.
 *  - Else infer roles (ISSUE → manager->controller; others → controller only).
 *  - Parse stamps from row.Note/Details like "[Approved by Manager ...]" to mark steps.
 *  - If still Pending, first non-Approved step becomes current.
 *  - Names/emails are looked up from "Users" sheet (Active users with Role=step.role).
 */
function getApproverFlow(pendingId) {
  pendingId = String(pendingId || "").trim();
  if (!pendingId) throw new Error("Missing PendingID");

  var ss = SpreadsheetApp.getActive();
  var sh = ss.getSheetByName("Pending");
  if (!sh) throw new Error('Sheet "Pending" not found');

  var values = sh.getDataRange().getValues();
  if (!values.length) throw new Error('No data in "Pending"');

  var head = values[0].map(String);
  var idx = function (k) { return head.indexOf(k); };

  var iPendingID = idx("PendingID");
  if (iPendingID < 0) throw new Error('Column "PendingID" not found in "Pending"');

  var iType      = idx("Type");
  var iStatus    = idx("Status");
  var iNote      = idx("Note");
  var iDetails   = idx("Details");
  var iPayload   = idx("PayloadJSON");
  var iNextRole  = idx("NextRole");           // <-- new
  var iCurAppr   = idx("CurrentApprover");    // optional
  var iApprCsv   = idx("ApproversCSV");       // optional

  var rowIndex = -1;
  for (var r = 1; r < values.length; r++) {
    if (String(values[r][iPendingID]) === pendingId) { rowIndex = r; break; }
  }
  if (rowIndex < 0) throw new Error("PendingID not found: " + pendingId);

  var row     = values[rowIndex];
  var type    = iType    > -1 ? String(row[iType])    : "";
  var status  = iStatus  > -1 ? String(row[iStatus])  : "";
  var note    = iNote    > -1 ? String(row[iNote]||""): "";
  var details = iDetails > -1 ? String(row[iDetails]||""): "";
  var nextRole= iNextRole>-1 ? String(row[iNextRole]||"").toLowerCase() : "";
  var curName = iCurAppr >-1 ? String(row[iCurAppr] || "") : "";
  var apprCsv = iApprCsv >-1 ? String(row[iApprCsv] || "") : "";

  var payload = null;
  if (iPayload > -1 && row[iPayload]) {
    try { payload = JSON.parse(row[iPayload]); } catch (e) {}
  }

  // 1) Prefer structured payload flow
  var flow = [];
  var fromPayload = payload && (payload.approval && payload.approval.flow ||
                                payload.request && payload.request.approvers ||
                                payload.approvers);
  if (Array.isArray(fromPayload) && fromPayload.length) {
    flow = fromPayload.map(function (a) {
      return {
        role:      (a.role || a.Role || "").toString().toLowerCase(),
        name:      a.name || a.Name || "",
        email:     a.email || a.Email || "",
        status:    a.status || a.Status || "Pending",
        at:        a.at || a.At || "",
        isCurrent: !!(a.isCurrent || a.Current)
      };
    });
  }

  // 2) Else, build flow from ApproversCSV/CurrentApprover if available
  if (!flow.length && apprCsv) {
    var names = apprCsv.split(",").map(function(s){return String(s).trim();}).filter(Boolean);
    flow = names.map(function(n){
      return {
        role: "",                  // unknown from CSV
        name: n,
        email: "",
        status: "Pending",
        at: "",
        isCurrent: curName && n.toLowerCase() === curName.toLowerCase()
      };
    });
  }

  // 3) Else, infer roles by Type
  if (!flow.length) {
    var roles = (String(type) === "ISSUE")
      ? ["manager", "controller"]
      : ["controller"];
    flow = roles.map(function (role) {
      return { role: role, name: "", email: "", status: "Pending", at: "", isCurrent: false };
    });
  }

  // 4) Apply stamps from Note/Details to set status per role/name
  var blob = (note + "\n" + details).toLowerCase();
  function has(needle){ return blob.indexOf(needle) > -1; }

  flow.forEach(function (step) {
    // Try by role
    if (step.role) {
      if (has("[approved by " + step.role)) step.status = "Approved";
      else if (has("[declined by " + step.role)) step.status = "Declined";
      else if (has("[voided by " + step.role)) step.status = "Voided";
    }
    // Try by name as well (if role absent)
    if (!step.role && step.name) {
      var nm = step.name.toLowerCase();
      if (has("[approved by " + nm)) step.status = "Approved";
      else if (has("[declined by " + nm)) step.status = "Declined";
      else if (has("[voided by " + nm)) step.status = "Voided";
    }
  });

  // 5) If whole request is Declined/Voided, reflect that on the first non-approved step
  if (/^declined$/i.test(status)) {
    var d = flow.find(function (s){ return s.status !== "Approved"; });
    if (d) d.status = "Declined";
  } else if (/^voided$/i.test(status)) {
    var v = flow.find(function (s){ return s.status !== "Approved"; });
    if (v) v.status = "Voided";
  }

  // 6) Mark "current" based on NextRole (this mirrors your emails)
  flow.forEach(function (s){ s.isCurrent = false; });
  if (/^pending$/i.test(status) && nextRole) {
    // First try to find that role
    var cur = flow.find(function (s){ return String(s.role||"").toLowerCase() === nextRole; });
    if (cur) {
      cur.isCurrent = true;
    } else {
      // If our flow doesn’t include it (e.g., CSV names only), inject a step for visibility
      flow.unshift({ role: nextRole, name: "", email: "", status: "Pending", at: "", isCurrent: true });
    }
  } else {
    // Fallback: the first Pending one is current
    var p = flow.find(function (s){ return s.status === "Pending"; });
    if (p) p.isCurrent = true;
  }

  // 7) If we only have roles, enrich with directory (Users sheet)
  try {
    var directory = _getActiveUsersByRole_(); // { role:[{name,email}...] }
    flow.forEach(function (s) {
      if (!s) return;
      if (!s.role) return; // CSV-only rows might not have a role
      var list = directory[s.role] || [];
      if (list.length) {
        if (!s.name)  s.name  = list.map(function(u){ return u.name; }).join(", ");
        if (!s.email) s.email = list.length === 1 ? list[0].email : "";
      }
    });
  } catch (e) {}

  return flow;
}


/**
 * Build { role: [{name,email}], ... } from "Users" sheet.
 * Expected headers: Name, Email, Role, Status
 */
function _getActiveUsersByRole_() {
  var ss = SpreadsheetApp.getActive();
  var sh = ss.getSheetByName("Users");
  var out = {};
  if (!sh) return out;

  var values = sh.getDataRange().getValues();
  if (!values.length) return out;

  var head = values[0].map(String);
  var iName   = head.indexOf("Name");
  var iEmail  = head.indexOf("Email");
  var iRole   = head.indexOf("Role");
  var iStatus = head.indexOf("Status");

  for (var r = 1; r < values.length; r++) {
    var status = iStatus > -1 ? String(values[r][iStatus]) : "Active";
    if (/^disabled$/i.test(status) || /^pending$/i.test(status) || /^no$/i.test(status)) continue;

    var role = iRole > -1 ? String(values[r][iRole]).toLowerCase() : "";
    if (!role) continue;

    if (!out[role]) out[role] = [];
    out[role].push({
      name:  iName  > -1 ? String(values[r][iName])  : "",
      email: iEmail > -1 ? String(values[r][iEmail]) : ""
    });
  }
  return out;
}

function buildApproverFlow_(pen){
  // Steps by type:
  //  - RECEIVE: controller only
  //  - ISSUE / REQUEST: manager → controller
  var type = String(pen && pen.Type || '').toUpperCase();
  var steps = (type === 'RECEIVE') ? ['controller'] : ['manager','controller'];

  var hist = [];
  try { hist = pen && pen.ApprovalsJSON ? JSON.parse(pen.ApprovalsJSON) : []; } catch(e){ hist = []; }
  var curRole = String(pen && pen.NextRole || '').toLowerCase();

  // Helper to find approvals/declines per role
  function findForRole(role){
    role = String(role||'').toLowerCase();
    var dec = hist.find(function(h){ return String(h.role||'').toLowerCase() === role && h.declined; });
    var app = hist.find(function(h){ return String(h.role||'').toLowerCase() === role && !h.declined; });
    if (dec) return { status:'Declined', at: dec.at || '', comment: dec.reason || dec.comment || '' };
    if (app) return { status:'Approved', at: app.at || '', comment: app.comment || '' };
    return { status:'Pending', at:'', comment:'' };
  }

  // Return compact, group-level flow (labels match what the UI shows)
  return steps.map(function(role, idx){
    var res = findForRole(role);
    return {
      name: role === 'manager' ? 'Managers' : 'Controllers',
      email: '',
      status: res.status,    // Pending | Approved | Declined
      at: res.at,
      comment: res.comment,
      isCurrent: (res.status === 'Pending' && curRole === role)
    };
  });
}

// NEW: look up a user's role by email (Active users only)
function getRoleByEmail(email){
  if(!email) return '';
  const row = _readObjects(sheet(SHEET_USERS))
    .find(u => String(u.Email).toLowerCase() === String(email).toLowerCase() && String(u.Status) === 'Active');
  return row ? String(row.Role || '').toLowerCase() : '';
}

/* ---------------- Price helpers ---------------- */
function recordPrice_(sku, price, source, note, linkId, pendingId) {
  if (!(price > 0)) return; // ignore empty/invalid
  const me = getCurrentUser();
  _append(sheet(SHEET_PRICES), {
    SKU: sku,
    Price: Number(price),
    At: nowISO(),
    By: me.email || '',
    Source: source || '',
    Note: note || '',
    LinkID: linkId || '',
    PendingID: pendingId || ''
  });
  // keep latest on Items for fast reads
  _updateByKey(sheet(SHEET_ITEMS), 'SKU', sku, { UnitPrice: Number(price), UpdatedAt: nowISO() });
}

function getPriceHistoryMap_() {
  const rows = _readObjects(sheet(SHEET_PRICES));
  const map = {};
  rows.forEach(r => {
    const k = String(r.SKU || '');
    if (!k) return;
    (map[k] = map[k] || []).push({
      price: Number(r.Price || 0),
      at: r.At,
      by: r.By,
      source: r.Source || '',
      note: r.Note || '',
      linkId: r.LinkID || '',
      pendingId: r.PendingID || ''
    });
  });
  // newest first
  Object.keys(map).forEach(k => map[k].sort((a,b)=> new Date(b.at) - new Date(a.at)));
  return map;
}

function computePriceStats_(arr) {
  if (!arr || !arr.length) return { current:null, average:null, count:0 };
  const current = arr[0].price;
  const sum = arr.reduce((a,b)=> a + Number(b.price||0), 0);
  return { current, average: (arr.length ? (sum/arr.length) : null), count: arr.length };
}

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

function backfillNotesWithMetaLines() {
  const sheets = [
    { sh: sheet(SHEET_PENDING), key: 'PendingID' },
    { sh: sheet(SHEET_LEDGER),  key: 'ID' }
  ];

  sheets.forEach(({sh, key}) => {
    const rows = _readObjects(sh);
    rows.forEach(r => {
      let meta = null;
      try {
        const p = r.PayloadJSON ? JSON.parse(r.PayloadJSON) : null;
        meta = p && (p.meta || (p.employee || p.department || p.businessUnit || p.deploymentLocation ? p : null));
      } catch(e){ /* ignore */ }

      if (!meta) return;

      const issuedToLine = (meta.employee || meta.department)
        ? `Issued To: ${safe(meta.employee || '—')} (${safe(meta.department || '—')})` : '';
      const buLine = meta.businessUnit ? `Business Unit: ${safe(meta.businessUnit)}` : '';
      const deployLine = meta.deploymentLocation ? `Deployment: ${safe(meta.deploymentLocation)}` : '';

      const want = [issuedToLine, buLine, deployLine].filter(Boolean);
      if (!want.length) return;

      const current = String(r.Note || '');
      const hasAll = want.every(w => current.indexOf(w) >= 0);
      if (hasAll) return;

      const newNote = appendNoteUnique_(current, want.join('\n'));
      _updateByKey(sh, key, r[key], { Note: newNote });
    });
  });

  return { ok:true };
}

// --- Note helpers (dedupe stamps & lines) ---
function stamp_(verb, who, tail) {
  // Formats a short review/request stamp, e.g. "[Approved by a@b @ 1/2/2025, 3:45 PM] Comment: ok"
  var whenLocal = new Date().toLocaleString();
  var w = String(who || '—');
  var t = tail ? String(tail) : '';
  return '[' + verb + ' by ' + w + ' @ ' + whenLocal + ']' + t;
}

function appendNoteUnique_(existing, addition) {
  var base = String(existing || '').trim();
  var add  = String(addition || '').trim();
  if (!add) return base;
  if (!base) return add;
  // Avoid adding duplicate stamp/line
  return base.indexOf(add) >= 0 ? base : (base + '\n' + add);
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
  const base = _readObjects(sheet(SHEET_PENDING)).filter(p => p.Status === 'Pending');

  // Helper: decorate for UI (Current Approver label + role token)
  function decorate(rows){
    return rows.map(r => {
      const role = String(r.NextRole || '').toLowerCase();
      const label = role === 'manager' ? 'Managers'
                  : role === 'controller' ? 'Controllers'
                  : '';
      return Object.assign({}, r, {
        CurrentApprover: label,
        CurrentApproverRole: role
      });
    });
  }

  // Controllers: everything
  if (me.role === 'controller' && me.status === 'Active') return decorate(base);

  // Managers: approvables + mine (dedup)
  if (me.role === 'manager' && me.status === 'Active') {
    const mine = base.filter(p => String(p.By || '').toLowerCase() === String(me.email || '').toLowerCase());
    const approvables = base.filter(p => getRoleByEmail(p.By) !== 'manager');
    const seen = new Set();
    return decorate(approvables.concat(mine).filter(r => { const k = r.PendingID; if (seen.has(k)) return false; seen.add(k); return true; }));
  }

  // Users: only theirs
  if (me.status === 'Active' && me.email) {
    return decorate(base.filter(p => String(p.By || '').toLowerCase() === String(me.email || '').toLowerCase()));
  }

  return [];
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

  // Read all rows for the user
  const allPending = _readObjects(sheet(SHEET_PENDING))
    .filter(r => String(r.By || '').toLowerCase() === email);
  const allLedger  = _readObjects(sheet(SHEET_LEDGER))
    .filter(r => String(r.By || '').toLowerCase() === email);

  // What the UI shows in "My Requests" table = only still-pending requests
  let minePending = allPending
    .filter(r => String(r.Status) === 'Pending')
    .map(function(r){
      // Keep the compact stage flow for the Request Details modal
      const flow = buildApproverFlow_(r);
      // Also surface Requested Items for the modal “Item” list
      let itemsList = [];
      try {
        const p = r && r.PayloadJSON ? JSON.parse(r.PayloadJSON) : null;
        if (p && Array.isArray(p.items) && p.items.length) {
          itemsList = p.items.map(it => (it.Name || it.name || it.SKU || '').toString()).filter(Boolean);
        } else if (r.Name) {
          itemsList = [String(r.Name)];
        }
      } catch(e){}
      return Object.assign({}, r, { Approvers: flow, RequestedItems: itemsList });
    });

  // Ledger stays raw; UI doesn’t render it directly for pending list
  const mineLedger = allLedger;

  // Pre-compute SLA (distinct requests)
  const mySLA = computeMySLA_(email);

  return { minePending, mineLedger, mySLA };
}


function getBootstrap() {
  // Make columns E..H = RequestedRole, Role, Status, CreatedAt
  normalizeUsersSheet();
  ensureUsersTimestampColumnAndBackfill();

  const me = getCurrentUser();
  const info = getSpreadsheetInfo();

  const usersPending = (me.role === 'controller' && me.status === 'Active')
    ? _readObjects(sheet(SHEET_USERS)).filter(u => u.Status === 'Pending')
    : [];

  const my = getMyActivity();

  let businessUnits = [], departments = [], deploymentLocations = [], categories = [];
  try { businessUnits = _readObjects(sheet(SHEET_BUS_UNITS)); } catch(e){}
  try { departments   = _readObjects(sheet(SHEET_DEPTS)); } catch(e){}
  try { deploymentLocations  = _readObjects(sheet(SHEET_DEPLOY_LOCS)); } catch(e){}
  try { categories    = _readObjects(sheet(SHEET_CATEGORIES)); } catch(e){} 
  // Enrich items with price history + rollups
  let items = getItems();
  let priceMap = {};
  try { priceMap = getPriceHistoryMap_(); } catch(e){ priceMap = {}; }
  items = items.map(it => {
    const ph = priceMap[it.SKU] || [];
    const stats = computePriceStats_(ph);
    return {
      ...it,                          // includes it.Category from sheet
      PriceHistory: ph,
      PriceCurrent: stats.current,
      PriceAverage: stats.average,
      PriceCount: stats.count
    };
  });

  return {
    user: me,
    counts: getCounts(),
    items: items.map(it => Object.assign({}, it, { ItemDescription: it.Description || '' })), // helpful alias
    pending: getPending(),
    usersPending,
    ledger: getLedger(500),
    minePending: my.minePending,
    mineLedger:  my.mineLedger,
    mySLA:       my.mySLA,
    db: info,
    businessUnits,
    departments,
    deploymentLocations,
    categories,
    lookups: { businessUnits, departments, deploymentLocations, categories },
    serverNow: nowISO(),               // ⬅️ fresh timestamp for polling
    cacheBuster: Utilities.getUuid()   // ⬅️ always changes, guarantees UI sees a diff
  };
}

/* ---------------- SKU History (normalized; supports multi-item tx) ---------------- */
/* ---------------- SKU History (normalized; supports multi-item tx) ---------------- */
function getSkuHistory(sku) {
  if (!sku) throw new Error('Missing SKU');
  sku = String(sku).trim();

  const ledRows = _readObjects(sheet(SHEET_LEDGER));
  const penRows = _readObjects(sheet(SHEET_PENDING));
  const penByLink = Object.fromEntries(penRows.map(p => [String(p.LinkID), p]));

  // Price history (for initial price on creation)
  let priceMap = {};
  try { priceMap = getPriceHistoryMap_(); } catch(e){ priceMap = {}; }

  function splitSkus(cell) {
    return String(cell || '')
      .split(',')
      .map(s => s.trim())
      .filter(Boolean);
  }

  function addStamps(baseNote, l, p) {
    const note = String(baseNote || '').trim();
    const addLines = [];

    // Normalize helpers
    const norm = s => String(s || '').trim().toLowerCase();
    const noteNorm = norm(note);
    const hasSameRemark = (txt) => {
      if (!txt) return false;
      const r = norm(txt);
      return noteNorm.includes('remarks: ' + r) ||
             noteNorm.includes('remark: '  + r) ||
             noteNorm.includes('notes: '   + r) ||
             noteNorm.includes('note: '    + r);
    };

    // Requester meta (Reason / Remarks)
    try {
      const pj = p && p.PayloadJSON ? JSON.parse(p.PayloadJSON) : null;
      const reason  = pj && pj.reason ? String(pj.reason).trim() : (p && p.Reason ? String(p.Reason).trim() : '');
      const remarks = pj && pj.note   ? String(pj.note).trim()   : '';
      if (reason && !noteNorm.includes(('reason: ' + norm(reason)))) addLines.push('Reason: ' + reason);
      if (remarks && !hasSameRemark(remarks)) addLines.push('Remarks: ' + remarks);
    } catch(e){}

    // Submitted
    if (p) {
      const tag = '[Submitted by ';
      if (!note.includes(tag)) {
        const whenLocal = p.When ? new Date(p.When).toLocaleString() : '';
        addLines.push(`[Submitted by ${p.By || '—'} @ ${whenLocal}]`);
      }
    }

    // Review
    const reviewedTag =
      (l.Status === 'Approved') ? '[Approved by ' :
      (l.Status === 'Declined') ? '[Declined by ' :
      (l.Status === 'Voided')   ? '[Voided by '   : null;

    if (reviewedTag && !note.includes(reviewedTag)) {
      const ts = l.ReviewedAt ? new Date(l.ReviewedAt).toLocaleString() : '';
      const tail =
        (l.Status === 'Declined' || l.Status === 'Voided')
          ? ((p && p.Reason) ? ` — Reason: ${String(p.Reason).trim()}` : '')
          : '';
      addLines.push(`${reviewedTag}${l.ReviewedBy || '—'} @ ${ts}]${tail}`);
    }

    // Creation hint + initial price (if CREATE_SKU)
    if (String(l.Type) === 'CREATE_SKU') {
      if (!/created\b/i.test(note)) addLines.push('Created');
      try {
        const ph = priceMap[sku] || [];
        const hit = ph.find(x => (x.source === 'CREATE_SKU') && (x.linkId === l.ID || x.pendingId === (p && p.PendingID)));
        if (hit && Number(hit.price) > 0) addLines.push('Initial Price: ' + formatPHP_(hit.price));
      } catch(e){}
    }

    return appendNoteUnique_(note, addLines.filter(Boolean).join('\n'));
  }

  const out = [];

  ledRows.forEach(l => {
    const skus = splitSkus(l.SKU);
    const isMulti = skus.length > 1;
    const p = penByLink[l.ID] || null;

    // --- Single-item rows ---
    if (!isMulti) {
      if (skus[0] !== sku) return;
      const delta = Number(l.Delta || 0);
      const noteWithStamps = addStamps(l.Note, l, p);

      out.push({
        linkId:   l.ID,
        pendingId:(p && p.PendingID) || '',
        when:     l.When,
        type:     l.Type,
        qty:      String(l.Type) === 'CREATE_SKU' ? '' : Math.abs(delta),
        delta:    String(l.Type) === 'CREATE_SKU' ? 0 : delta,
        uom:      l.UoM || '',
        by:       l.By || '',
        status:   l.Status || '',
        note:     noteWithStamps,
        item:     l.Item || ''
      });
      return;
    }

    // --- Multi-item rows (batch) ---
    if (!skus.includes(sku)) return;

    let itemRec = null;
    try {
      const payload = p && p.PayloadJSON ? JSON.parse(p.PayloadJSON) : null;
      if (payload && Array.isArray(payload.items)) {
        itemRec = payload.items.find(it => String(it.SKU) === sku) || null;
      }
    } catch (e) { /* ignore */ }

    let qty = null, uom = '';
    if (itemRec) {
      qty = (String(l.Type) === 'CREATE_SKU') ? null : Number(itemRec.qty || 0);
      uom = itemRec.UoM || '';
    } else {
      // Fallback parse (applies to RECEIVE / ISSUE / CREATE_SKU lines)
      const line = String(l.Note || '')
        .split('\n')
        .map(s => s.trim())
        .find(s => s.endsWith('(' + sku + ')'));
      if (line) {
        // Pattern emitted by _summarizeMulti_ for all multi ops (qty is "0" for CREATE_SKU)
        const m = line.match(/^\d+\.\s*([\d.]+)\s+([^\s]+)\s+—/);
        if (m) {
          qty = (String(l.Type) === 'CREATE_SKU') ? null : Number(m[1]);
          uom = m[2];
        }
      }
    }

    const sign = (String(l.Type) === 'ISSUE') ? -1 : (String(l.Type) === 'RECEIVE' ? +1 : 0);
    const delta = (qty != null) ? sign * qty : 0;
    const noteWithStamps = addStamps(l.Note, l, p);

    out.push({
      linkId:    l.ID,
      pendingId: (p && p.PendingID) || '',
      when:      l.When,
      type:      l.Type,
      qty:       (qty != null ? qty : ''),
      delta:     (qty != null ? delta : 0),
      uom:       uom || (l.UoM === 'mixed' ? '' : l.UoM || ''),
      by:        l.By || '',
      status:    l.Status || '',
      note:      noteWithStamps,
      item:      l.Item || ''
    });
  });

  // ===== NEW: Ensure a CREATE_SKU record exists (synthesize if missing) =====
  const hasCreate = out.some(r => String(r.type) === 'CREATE_SKU');
  if (!hasCreate) {
    const it = _findBy(sheet(SHEET_ITEMS), 'SKU', sku);
    if (it) {
      // Try to attach an "initial price": prefer a price with source CREATE_SKU,
      // otherwise fall back to the oldest price we have.
      let initPriceNote = '';
      try {
        const ph = (priceMap[sku] || []).slice();         // newest-first by getPriceHistoryMap_
        const createHit = ph.find(x => x.source === 'CREATE_SKU');
        const oldest = ph.length ? ph[ph.length - 1] : null;
        const pick = createHit || oldest;
        if (pick && Number(pick.price) > 0) {
          initPriceNote = '\nInitial Price: ' + formatPHP_(pick.price);
        }
      } catch(e){}

      out.push({
        linkId:   '',                 // no ledger ID (synthetic)
        pendingId:'',
        when:     it.CreatedAt || it.UpdatedAt || nowISO(),
        type:     'CREATE_SKU',
        qty:      '',
        delta:    0,
        uom:      it.UoM || '',
        by:       '',                 // unknown
        status:   'Approved',         // treat as established
        note:     ('Created (inferred)' + initPriceNote).trim(),
        item:     it.Name || ''
      });
    }
  }
  // ===== END NEW =====

  out.sort((a, b) => new Date(b.when) - new Date(a.when));
  return out;
}




/* ---------------- Queue Pending ---------------- */
function queuePending(rec) {
  const me = getCurrentUser();
  if (!me.email || me.status !== 'Active') throw new Error('Not authorized.');

  const linkId    = nextTrxId();
  const pendingId = linkId + '-P';

  const baseNote = rec.note ? String(rec.note) : '';
  const noteNorm = baseNote.toLowerCase();

  const reqBits = [];
  if (rec.reason) {
    const r = String(rec.reason).trim();
    if (!noteNorm.includes(`reason: ${r.toLowerCase()}`)) reqBits.push('Reason: ' + r);
  }
  if (rec.note) {
    const n = String(rec.note).trim();
    if (!noteNorm.includes(`remarks: ${n.toLowerCase()}`)) reqBits.push('Remarks: ' + n);
  }

  // Price (single-item create/modify only, if provided)
  const payloadPrice = (rec && rec.payload && Number(rec.payload.price) > 0) ? Number(rec.payload.price) : null;
  const priceLine = payloadPrice ? ('Unit Price: ' + formatPHP_(payloadPrice)) : '';

  // meta
  const meta = rec && rec.payload && rec.payload.meta ? rec.payload.meta : null;
  const issuedToLine = (meta && (meta.employee || meta.department))
    ? `Issued To: ${safe(meta.employee || '—')} (${safe(meta.department || '—')})`
    : '';
  const buLine = (meta && meta.businessUnit) ? `Business Unit: ${safe(meta.businessUnit)}` : '';
  const deployLine = (meta && meta.deploymentLocation) ? `Deployment: ${safe(meta.deploymentLocation)}` : '';

  const submitStamp = stamp_('Submitted', me.email, '');

  // Note (include price line if present)
  const noteWithMeta = appendNoteUnique_(
    [priceLine, baseNote, issuedToLine, buLine, deployLine, reqBits.join(' | ')].filter(Boolean).join('\n'),
    submitStamp
  );

  // Details (include price at the front when present)
  const details = [
    priceLine,
    issuedToLine, buLine, deployLine,
    reqBits.join(' | '),
    submitStamp
  ].filter(Boolean).join(' | ');

  _append(sheet(SHEET_LEDGER), {
    ID: linkId, When: nowISO(), Type: rec.type,
    SKU: rec.sku || '', Item: rec.name || '',
    Delta: rec.delta || 0, UoM: rec.uom || '',
    Status: 'Pending', By: me.email,
    Note: noteWithMeta
  });

    // decide staged flow
  const requesterRole = String(me.role || '').toLowerCase();
  const stage1Role =
    (rec.type === 'RECEIVE') ? 'controller' :
    (rec.type === 'ISSUE'   || rec.type === 'REQUEST')
                             ? (requesterRole === 'manager' ? 'controller' : 'manager') :
    '';


  _append(sheet(SHEET_PENDING), {
    PendingID: pendingId, LinkID: linkId, When: nowISO(),
    Type: rec.type, SKU: rec.sku || '', Details: details,
    Name: rec.name || '', UoM: rec.uom || '', Qty: rec.qty || 0,
    Delta: rec.delta || 0, Reason: rec.reason || '',
    Note: noteWithMeta, By: me.email, Status: 'Pending',
    PayloadJSON: rec.payload ? JSON.stringify(rec.payload) : '',
    Stage: 1,                          // ⬅ NEW
    NextRole: stage1Role,              // ⬅ NEW
    ApprovalsJSON: JSON.stringify([])  // ⬅ NEW
  });


  notifyPendingCreated({ linkId, pendingId, rec, by: me.email });
  return { ok:true, pendingId, linkId };
}

function queuePendingMulti(rec) {
  const me = getCurrentUser();
  if (!me.email || me.status !== 'Active') throw new Error('Not authorized.');
  if (!rec || !Array.isArray(rec.items) || rec.items.length === 0) throw new Error('No items supplied.');
  if (!['RECEIVE','ISSUE','REQUEST','CREATE_SKU'].includes(String(rec.type))) throw new Error('Unsupported type for multi: ' + rec.type);

  const linkId    = nextTrxId();
  const pendingId = linkId + '-P';

  const totalDelta = (rec.type === 'CREATE_SKU')
    ? 0
    : rec.items.reduce((a,b)=> a + Number(b.delta||0), 0);

  const skus       = rec.items.map(i => i.SKU).filter(Boolean);
  const uniqueUoms = Array.from(new Set(rec.items.map(i => i.UoM).filter(Boolean)));
  const uomCell    = (uniqueUoms.length === 1 ? uniqueUoms[0] : 'mixed');

  const sum = _summarizeMulti_(rec.type, rec.items, rec.note);
  const submitStamp = stamp_('Submitted', me.email, '');
  // Build a CSV of names so the Item cell always shows them even if the UI parser misses CREATE_SKU notes
  const namesCsv = rec.items
    .map(i => (i.Name != null ? i.Name : i.name) || '')
    .filter(Boolean)
    .join(', ');
  const itemCell = namesCsv ? (sum.title + '\n' + namesCsv) : sum.title;

  const noteNorm = String(rec.note || '').toLowerCase();
  const reqMeta = [];
  if (rec.reason) {
    const r = String(rec.reason).trim();
    if (!noteNorm.includes(`reason: ${r.toLowerCase()}`)) reqMeta.push('Reason: ' + r);
  }
  if (rec.note) {
    const n = String(rec.note).trim();
    if (!noteNorm.includes(`remarks: ${n.toLowerCase()}`)) reqMeta.push('Remarks: ' + n);
  }

  // NEW: meta lines for Notes (from rec.meta)
  const meta = rec && rec.meta ? rec.meta : null;
  const issuedToLine = (meta && (meta.employee || meta.department))
    ? `Issued To: ${safe(meta.employee || '—')} (${safe(meta.department || '—')})`
    : '';
  const buLine = (meta && meta.businessUnit) ? `Business Unit: ${safe(meta.businessUnit)}` : '';
  const deployLine = (meta && meta.deploymentLocation) ? `Deployment: ${safe(meta.deploymentLocation)}` : '';

  const ledgerNote = appendNoteUnique_(
    [sum.listText, issuedToLine, buLine, deployLine, reqMeta.join(' | ')].filter(Boolean).join('\n'),
    submitStamp
  );

  const details = [
    sum.listText.replace(/\n/g, ' | '),
    issuedToLine, buLine, deployLine,
    reqMeta.join(' | '),
    submitStamp
  ].filter(Boolean).join(' | ');

  _append(sheet(SHEET_LEDGER), {
    ID: linkId, When: nowISO(), Type: rec.type,
    SKU: skus.join(', '), Item: sum.title,
    Delta: totalDelta,
    UoM: uomCell,
    Status: 'Pending', By: me.email, Note: ledgerNote
  });

    const requesterRole = String(me.role || '').toLowerCase();
  const stage1Role =
    (rec.type === 'RECEIVE') ? 'controller' :
    (rec.type === 'ISSUE'   || rec.type === 'REQUEST')
                             ? (requesterRole === 'manager' ? 'controller' : 'manager') :
    '';


  _append(sheet(SHEET_PENDING), {
    PendingID: pendingId, LinkID: linkId, When: nowISO(),
    Type: rec.type, SKU: skus.join(', '), Details: details,
    Name: '', UoM: uomCell, Qty: '', Delta: totalDelta,
    Reason: rec.reason || '', Note: ledgerNote, By: me.email, Status: 'Pending',
    PayloadJSON: JSON.stringify({
      type: rec.type,
      items: rec.items,
      note: rec.note || '',
      reason: rec.reason || '',
      meta: meta || null
    }),
    Stage: 1,                          // ⬅ NEW
    NextRole: stage1Role,              // ⬅ NEW
    ApprovalsJSON: JSON.stringify([])  // ⬅ NEW
  });


  notifyPendingCreated({
    linkId, pendingId,
    rec: { type: rec.type, sku: skus.join(', '), name: sum.title, uom: uomCell, delta: totalDelta, note: rec.note || '' },
    by: me.email
  });

  return { ok:true, pendingId, linkId };
}




/* ---------------- Actions ---------------- */
// === Requester-only edit/cancel (Pending only), aligned to existing flow ===

function _requesterOwnsPending_(pen) {
  const me = getCurrentUser();
  return !!(me.email && me.status === 'Active' &&
            String(pen.By || '').toLowerCase() === String(me.email).toLowerCase() &&
            String(pen.Status) === 'Pending');
}

function _firstApproverRole_(type, requesterRole){
  type = String(type||'').toUpperCase();
  const r = String(requesterRole||'').toLowerCase();
  if (type === 'RECEIVE') return 'controller';
  if (type === 'ISSUE' || type === 'REQUEST') return (r === 'manager' ? 'controller' : 'manager');
  return '';
}

/**
 * Lightweight read model for editing a pending request (requester-only, Pending only).
 * Returns { ok, pendingId, model:{ type, reason, note, meta, items[] } }
 */
function getPendingForEdit(pendingId){
  const pen = _findBy(sheet(SHEET_PENDING), 'PendingID', pendingId);
  if (!pen) throw new Error('Request not found.');
  if (!_requesterOwnsPending_(pen)) throw new Error('Only the requester can edit while status is Pending.');

  let payload = {};
  try { payload = pen.PayloadJSON ? JSON.parse(pen.PayloadJSON) : {}; } catch(e){}

  const items = Array.isArray(payload.items) && payload.items.length
    ? payload.items
    : (pen.SKU ? [{
        SKU: pen.SKU, Name: pen.Name, UoM: pen.UoM,
        qty: Math.abs(Number(pen.Qty||0)),
        delta: Number(pen.Delta||0)
      }] : []);

  return {
    ok:true,
    pendingId,
    model: {
      type: pen.Type,
      reason: payload.reason ?? pen.Reason ?? '',
      note: payload.note ?? '',
      meta: payload.meta ?? null,
      items
    }
  };
}

/**
 * Submit an edit: voids old Pending, stamps Ledger "Edited #N",
 * creates a fresh Pending at Stage=1, NextRole reset, ApprovalsJSON=[],
 * and assigns a new PendingID by appending " (N)".
 */
function submitEditPending(pendingId, edited){
  const pen = _findBy(sheet(SHEET_PENDING), 'PendingID', pendingId);
  if (!pen) throw new Error('Request not found.');
  if (!_requesterOwnsPending_(pen)) throw new Error('Only the requester can edit while status is Pending.');

  const me = getCurrentUser();
  const type = String(pen.Type).toUpperCase();

  // Normalize incoming model
  const model  = edited && typeof edited === 'object' ? edited : {};
  const items  = Array.isArray(model.items) ? model.items : [];
  const reason = (model.reason ?? pen.Reason ?? '').toString();
  const note   = (model.note ?? '').toString();
  const meta   = model.meta ?? null;

  // 1) Void old Pending (audit kept)
  const stampIso = nowISO();
  const voidStamp = stamp_('Voided', me.email, ' — Edited & resubmitted');
  _updateByKey(sheet(SHEET_PENDING), 'PendingID', pendingId, {
    Status: 'Voided',
    ReviewedAt: stampIso,
    ReviewedBy: me.email,
    Note: appendNoteUnique_(pen.Note || '', voidStamp)
  });

  // 2) Stamp Ledger as "Edited #N"
  const led = _findBy(sheet(SHEET_LEDGER), 'ID', pen.LinkID);
  const siblings = _readObjects(sheet(SHEET_PENDING)).filter(r => String(r.LinkID) === String(pen.LinkID));
  const editSeq = siblings.length + 1;
  if (led) {
    _updateByKey(sheet(SHEET_LEDGER), 'ID', pen.LinkID, {
      Note: appendNoteUnique_(led.Note || '', `[Edited #${editSeq} by ${me.email} @ ${new Date().toLocaleString()}]`)
    });
  }

  // 3) Create fresh Pending row with reset flow
  const firstRole = _firstApproverRole_(type, me.role);

  const summarized = _summarizeMulti_(
    type,
    items.length ? items : [{
      SKU: pen.SKU, Name: pen.Name, UoM: pen.UoM,
      qty: Math.abs(Number(pen.Qty||0)), delta: Number(pen.Delta||0)
    }],
    note
  );

  const submitStamp = stamp_('Submitted', me.email, '');
  const newNote = appendNoteUnique_(
    [
      summarized.listText,
      (reason ? 'Reason: ' + reason : ''),
      (note ? 'Remarks: ' + note : '')
    ].filter(Boolean).join('\n'),
    submitStamp
  );
  const newDetails = [
    summarized.listText.replace(/\n/g,' | '),
    (reason ? 'Reason: ' + reason : ''),
    (note ? 'Remarks: ' + note : ''),
    submitStamp
  ].filter(Boolean).join(' | ');

  const skuCsv = items.length ? items.map(i => String(i.SKU||'').trim()).filter(Boolean).join(', ') : (pen.SKU || '');
  const totalDelta = items.length ? items.reduce((a,b)=> a + Number(b.delta||0), 0) : Number(pen.Delta||0);
  const uoms = items.length ? Array.from(new Set(items.map(i => String(i.UoM||'').trim()).filter(Boolean))) : (pen.UoM ? [pen.UoM] : []);
  const uomCell = uoms.length === 0 ? (pen.UoM || '') : (uoms.length === 1 ? uoms[0] : 'mixed');

  const basePid = String(pen.PendingID).replace(/\s*\(\d+\)\s*$/, '');
  const newPendingId = `${basePid} (${editSeq})`;

  _append(sheet(SHEET_PENDING), {
    PendingID: newPendingId,
    LinkID: pen.LinkID,                // keep the same ledger link
    When: nowISO(),
    Type: type,
    SKU: skuCsv,
    Details: newDetails,
    Name: summarized.title,
    UoM: uomCell,
    Qty: '',
    Delta: totalDelta,
    Reason: reason,
    Note: newNote,
    By: me.email,
    Status: 'Pending',
    PayloadJSON: JSON.stringify({ type, items, reason, note, meta }),
    Stage: 1,
    NextRole: firstRole,
    ApprovalsJSON: JSON.stringify([])
  });

  // notify first approver group again
  notifyPendingCreated({
    linkId: pen.LinkID,
    pendingId: newPendingId,
    rec: { type, sku: skuCsv, name: summarized.title, uom: uomCell, delta: totalDelta, note: newNote },
    by: me.email
  });

  return { ok:true, pendingId: newPendingId, linkId: pen.LinkID };
}

/** Requester-only cancel (void) their own Pending */
function cancelMyPending(pendingId, reason){
  const pen = _findBy(sheet(SHEET_PENDING), 'PendingID', pendingId);
  if (!pen) throw new Error('Request not found.');
  if (!_requesterOwnsPending_(pen)) throw new Error('Only the requester can cancel while status is Pending.');

  const me = getCurrentUser();
  const why = String(reason || 'Cancelled by requester').trim();
  const stampIso = nowISO();
  const cancelStamp = stamp_('Voided', me.email, ' — Reason: ' + why);

  _updateByKey(sheet(SHEET_PENDING), 'PendingID', pendingId, {
    Status: 'Voided',
    Reason: why,
    ReviewedAt: stampIso,
    ReviewedBy: me.email,
    Note: appendNoteUnique_(pen.Note || '', cancelStamp)
  });

  const led = _findBy(sheet(SHEET_LEDGER), 'ID', pen.LinkID);
  if (led) {
    _updateByKey(sheet(SHEET_LEDGER), 'ID', pen.LinkID, {
      Status: 'Voided',
      ReviewedAt: stampIso,
      ReviewedBy: me.email,
      Note: appendNoteUnique_(led.Note || '', cancelStamp)
    });
  }

  const updated = _findBy(sheet(SHEET_PENDING), 'PendingID', pendingId);
  notifyRequesterResult('Voided', updated);
  return { ok:true };
}

// Helper: look up current user's employee details from Users sheet
function getCurrentEmployee_() {
  var email = (Session.getActiveUser() && Session.getActiveUser().getEmail()) || '';
  email = String(email || '').trim().toLowerCase();
  var ss   = SpreadsheetApp.getActive();
  var sh   = ss.getSheetByName('Users');
  if (!sh) return { name: '', department: '', email: email };

  var values = sh.getDataRange().getValues(); // expects header row
  var h = values[0].map(String);
  var iEmail = h.indexOf('Email');
  var iName  = h.indexOf('Name');
  var iDept  = h.indexOf('Department');
  for (var r = 1; r < values.length; r++) {
    var rowEmail = String(values[r][iEmail] || '').trim().toLowerCase();
    if (rowEmail && rowEmail === email) {
      return {
        name: String(values[r][iName] || ''),
        department: String(values[r][iDept] || ''),
        email: email
      };
    }
  }
  return { name: '', department: '', email: email };
}

/**
 * Frontend entry: google.script.run.actionRequestItems(...)
 * Accepts:
 *   - payload: { items:[{SKU/sku, Qty/qty}], reason, requester:{name,email,department} }
 *   - OR legacy positional args (kept for compatibility)
 */
function actionRequestItems(payloadOrItems, employee, department, reason, businessUnit, deploymentLocation) {
  var items = [];
  var emp = employee, dept = department, rsn = reason, bu = businessUnit, depLoc = deploymentLocation;
  var explicitType = ''; // 'ISSUE' | 'REQUEST' | ''

  if (Array.isArray(payloadOrItems)) {
    items = payloadOrItems || [];
  } else if (payloadOrItems && typeof payloadOrItems === 'object') {
    items        = payloadOrItems.items || [];
    rsn          = payloadOrItems.reason;
    bu           = payloadOrItems.businessUnit;
    depLoc       = payloadOrItems.deploymentLocation;
    explicitType = String(payloadOrItems.type || '').toUpperCase();  // ← new

    // pull employee/department from payload.requester if present
    if (payloadOrItems.requester && typeof payloadOrItems.requester === 'object') {
      emp  = emp  || payloadOrItems.requester.name;
      dept = dept || payloadOrItems.requester.department;
      // requester.email is available if needed
    }

    // If still missing, resolve from Users by current session email
    if (!emp || !dept) {
      var who = getCurrentEmployee_();
      emp  = emp  || who.name;
      dept = dept || who.department;
    }
  } else {
    throw new Error('Invalid payload for actionRequestItems.');
  }

  if (!emp)  throw new Error('Employee is required.');
  if (!dept) throw new Error('Department is required.');

  // Normalize items (tolerate SKU/sku and Qty/qty/quantity/Quantity)
  var normalized = (items || [])
    .map(function(it) {
      var sku = String((it && (it.sku ?? it.SKU)) || '').trim();
      var rawQty = (it && (it.qty ?? it.Qty ?? it.quantity ?? it.Quantity));
      var qtyNum = Number(rawQty);
      return { sku: sku, qty: (Number.isFinite(qtyNum) ? qtyNum : NaN) };
    })
    .filter(function(it){ return it.sku && it.qty > 0; });

  if (normalized.length === 0) {
    throw new Error('No valid items provided.');
  }

  // Decide target action set by type; default to legacy Request
  var isIssue = (explicitType === 'ISSUE');

  if (normalized.length === 1) {
    var it = normalized[0];
    return isIssue
      ? actionIssue(it.sku, it.qty, emp, dept, rsn, bu, depLoc)
      : actionRequest(it.sku, it.qty, emp, dept, rsn, bu, depLoc);
  }

  return isIssue
    ? actionIssueMulti(normalized, emp, dept, rsn, bu, depLoc)
    : actionRequestMulti(normalized, emp, dept, rsn, bu, depLoc);
}

function actionRequest(sku, qty, employee, department, reason, businessUnit, deploymentLocation) {
  if (!sku || !(qty > 0)) throw new Error('Invalid request');
  const me = getCurrentUser();
  const it = _findBy(sheet(SHEET_ITEMS), 'SKU', sku);
  if (!it) throw new Error('SKU not found');
  if (String(it.Status) !== 'Active') throw new Error('Item must be Active to request.');
  if (Number(qty) > Number(it.Qty || 0)) throw new Error('Cannot request more than on-hand quantity');

  const parenthetical = `${department}${businessUnit ? ' | ' + businessUnit : ''}`;
  const note = `Requested for ${employee} (${parenthetical}).`;

  // Auto-approve for controllers (same as Issue)
  if (me.role === 'controller' && me.status === 'Active') {
    const itemsSh = sheet(SHEET_ITEMS);
    const toQty = Number(it.Qty || 0) - Math.abs(qty);
    if (toQty < 0) throw new Error('Cannot issue more than on-hand during finalize.');
    _updateByKey(itemsSh, 'SKU', sku, { Qty: toQty, UpdatedAt: nowISO() });

    const linkId = nextTrxId();
    _append(sheet(SHEET_LEDGER), {
      ID: linkId, When: nowISO(), Type:'REQUEST', SKU: sku, Item: it.Name,
      Delta: -Math.abs(qty), UoM: it.UoM, Status:'Approved', By: me.email,
      Note: appendNoteUnique_(note, stamp_('Fully Approved', me.email, ''))
    });

    const payload = { meta: { employee:String(employee||''), department:String(department||''), businessUnit: businessUnit?String(businessUnit):'', deploymentLocation: deploymentLocation?String(deploymentLocation):'' } };
    const pseudoPen = { Type:'REQUEST', SKU:sku, Name:it.Name, UoM:it.UoM, Qty:qty, Delta:-Math.abs(qty), By:me.email, Note:note, Reason:String(reason||''), PendingID:'', LinkID:linkId, PayloadJSON: JSON.stringify(payload) };
    notifyApprovedEvent('REQUEST', pseudoPen);
    notifyRequesterResult('Approved', pseudoPen);
    return { ok:true, linkId };
  }

  return queuePending({
    type: 'REQUEST',
    sku,
    name: it.Name,
    uom: it.UoM,
    qty,
    delta: -Math.abs(qty),
    reason,
    note,
    payload: {
      meta: {
        employee: String(employee || ''),
        department: String(department || ''),
        businessUnit: businessUnit ? String(businessUnit) : '',
        deploymentLocation: deploymentLocation ? String(deploymentLocation) : ''
      }
    }
  });
}

function actionRequestMulti(items, employee, department, reason, businessUnit, deploymentLocation) {
  if (!Array.isArray(items) || !items.length) throw new Error('No items to request.');
  if (!employee)   throw new Error('Employee is required.');
  if (!department) throw new Error('Department is required.');
  if (!reason)     throw new Error('Reason is required.');

  const me = getCurrentUser();
  const all = items.map(it => {
    const row = _findBy(sheet(SHEET_ITEMS), 'SKU', it.sku);
    if (!row) throw new Error('SKU not found: ' + it.sku);
    if (String(row.Status) !== 'Active') throw new Error('Item must be Active: ' + it.sku);
    const qty = Number(it.qty||0);
    if (!(qty > 0)) throw new Error('Invalid qty for ' + it.sku);
    if (qty > Number(row.Qty||0)) throw new Error('Insufficient stock for ' + it.sku);
    return { SKU: row.SKU, Name: row.Name, UoM: row.UoM, qty, delta: -Math.abs(qty) };
  });

  const parenthetical = `${department}${businessUnit ? ' | ' + businessUnit : ''}`;
  const note = `Requested for ${employee} (${parenthetical}).`;

  // Auto-approve for controllers (same as Issue)
  if (me.role === 'controller' && me.status === 'Active') {
    const itemsSh = sheet(SHEET_ITEMS);
    all.forEach(it => {
      const row = _findBy(itemsSh, 'SKU', it.SKU);
      const toQty = Number(row.Qty||0) + Number(it.delta||0); // delta negative
      if (toQty < 0) throw new Error('Cannot issue more than on-hand during finalize: ' + it.SKU);
      _updateByKey(itemsSh, 'SKU', it.SKU, { Qty: toQty, UpdatedAt: nowISO() });
    });

    const linkId = nextTrxId();
    const sum = _summarizeMulti_('ISSUE', all, note); // use Issue phrasing
    _immediateLedger_('REQUEST', linkId, { items: all, note }, sum.listText, all.map(b=>b.SKU).join(', '), sum.title, 'mixed', all.reduce((a,b)=>a+Number(b.delta||0),0));

    const payload = { meta: { employee:String(employee||''), department:String(department||''), businessUnit: businessUnit?String(businessUnit):'', deploymentLocation: deploymentLocation?String(deploymentLocation):'' } };
    const pseudoPen = { Type:'REQUEST', SKU: all.map(b=>b.SKU).join(', '), Name: sum.title, UoM:'mixed', Qty:'', Delta: all.reduce((a,b)=>a+Number(b.delta||0),0), By: me.email, Note: sum.listText, Reason:String(reason||''), PendingID:'', LinkID:linkId, PayloadJSON: JSON.stringify({items:all, ...payload}) };
    notifyApprovedEvent('REQUEST', pseudoPen);
    notifyRequesterResult('Approved', pseudoPen);
    return { ok:true, linkId };
  }

  return queuePendingMulti({
    type: 'REQUEST',
    items: all,
    note,
    reason,
    meta: {
      employee: String(employee || ''),
      department: String(department || ''),
      businessUnit: businessUnit ? String(businessUnit) : '',
      deploymentLocation: deploymentLocation ? String(deploymentLocation) : ''
    }
  });
}

function _immediateLedger_(type, linkId, payload, note, skuCsv, itemTitle, uomCell, deltaNum){
  _append(sheet(SHEET_LEDGER), {
    ID: linkId,
    When: nowISO(),
    Type: type,
    SKU: skuCsv,
    Item: itemTitle,
    Delta: deltaNum,
    UoM: uomCell,
    Status: 'Approved', // ✅ only Approved/Pending/Declined/Voided
    By: getCurrentUser().email,
    // Auto-final actions: keep a visible "[Fully Approved]" stamp in Note
    Note: appendNoteUnique_(note || '', stamp_('Fully Approved', getCurrentUser().email, ''))
  });
}



function actionCreateSku(payload) {
  if (!payload) throw new Error('Missing payload');
  if (!payload.sku) payload.sku = nextSkuId();

  // Upsert item
  const itemsSh = sheet(SHEET_ITEMS);
  ensureColumns(itemsSh, ['Category']);
  const existing = _findBy(itemsSh, 'SKU', payload.sku);
  const patch = {
    SKU: payload.sku,
    Name: payload.name || (existing && existing.Name) || '',
    Description: payload.desc || (existing && existing.Description) || '',
    Category: payload.category || payload.Category || (existing && existing.Category) || '',
    UoM: payload.uom || (existing && existing.UoM) || '',
    Location: payload.loc || (existing && existing.Location) || '',
    Qty: Number(existing ? (existing.Qty || 0) : 0),
    Status: existing ? (existing.Status || 'Active') : 'Active',
    CreatedAt: existing ? (existing.CreatedAt || nowISO()) : nowISO(),
    UpdatedAt: nowISO()
  };
  if (existing) _updateByKey(itemsSh, 'SKU', payload.sku, patch); else _append(itemsSh, patch);
  if (payload.price && Number(payload.price) > 0) {
    recordPrice_(payload.sku, Number(payload.price), 'CREATE_SKU', 'Initial price', '', '');
  }

  const linkId = nextTrxId();
  const note = `Create ${payload.sku} — ${payload.name}${payload.price ? ' ('+formatPHP_(payload.price)+')' : ''}`;
  _immediateLedger_('CREATE_SKU', linkId, payload, note, payload.sku, payload.name, payload.uom || '', 0);

  // Notify like "Approved — Create SKU"
  const pseudoPen = { Type:'CREATE_SKU', SKU:payload.sku, Name:payload.name, UoM:payload.uom||'', Qty:'', Delta:0, By:getCurrentUser().email, Note:note, Reason:'', PendingID:'', LinkID:linkId, PayloadJSON: JSON.stringify(payload) };
  notifyApprovedEvent('CREATE_SKU', pseudoPen);
  notifyRequesterResult('Approved', pseudoPen);
  return { ok:true, linkId, sku: payload.sku };
}



function actionCreateSkus(items, note) {
  if (!Array.isArray(items) || !items.length) throw new Error('No items to create.');

  const itemsSh = sheet(SHEET_ITEMS);
  ensureColumns(itemsSh, ['Category']);

  const batch = items.map(it => {
    const sku   = it.sku && String(it.sku).trim() ? String(it.sku).trim() : nextSkuId();
    const name  = String(it.name || '').trim(); if (!name) throw new Error('Item name is required for all rows.');
    const uom   = String(it.uom  || '').trim();
    const desc  = String(it.desc || '').trim();
    const loc   = String(it.loc  || '').trim();
    const cat   = String(it.category || it.Category || '').trim();
    const price = (it.price && Number(it.price) > 0) ? Number(it.price) : '';

    const existing = _findBy(itemsSh, 'SKU', sku);
    const patch = {
      SKU: sku, Name: name, Description: desc, Category: cat, UoM: uom, Location: loc,
      Qty: Number(existing ? (existing.Qty || 0) : 0),
      Status: existing ? (existing.Status || 'Active') : 'Active',
      CreatedAt: existing ? (existing.CreatedAt || nowISO()) : nowISO(),
      UpdatedAt: nowISO()
    };
    if (existing) _updateByKey(itemsSh, 'SKU', sku, patch); else _append(itemsSh, patch);
    if (price) recordPrice_(sku, price, 'CREATE_SKU', 'Initial price', '', '');
    return { SKU: sku, Name: name, UoM: uom, qty: 0, delta: 0, price };
  });

  const linkId = nextTrxId();
  const sum = _summarizeMulti_('CREATE_SKU', batch, note);
  _immediateLedger_('CREATE_SKU', linkId, { items: batch, note: String(note||'') }, sum.listText, batch.map(b=>b.SKU).join(', '), sum.title, 'mixed', 0);

  const pseudoPen = { Type:'CREATE_SKU', SKU: batch.map(b=>b.SKU).join(', '), Name: sum.title, UoM:'mixed', Qty:'', Delta:0, By:getCurrentUser().email, Note: sum.listText, Reason:'', PendingID:'', LinkID:linkId, PayloadJSON: JSON.stringify({items:batch}) };
  notifyApprovedEvent('CREATE_SKU', pseudoPen);
  notifyRequesterResult('Approved', pseudoPen);
  return { ok:true, linkId, created: batch.length };
}


function actionModifySku(payload) {
  if (!payload || !payload.sku) throw new Error('Missing SKU');

  const itemsSh = sheet(SHEET_ITEMS);
  const it = _findBy(itemsSh, 'SKU', payload.sku); if (!it) throw new Error('SKU not found');
  ensureColumns(itemsSh, ['Category']);

  // Apply patches (same validations as before)
  const statusTo = (payload && typeof payload.status !== 'undefined')
    ? (['Active','On Hold'].includes(String(payload.status)) ? String(payload.status) : it.Status)
    : it.Status;
  if (String(it.Status) === 'Retired') throw new Error('Cannot modify a Retired item.');

  _updateByKey(itemsSh, 'SKU', payload.sku, {
    Name:        (payload && payload.name) ?? it.Name,
    Description: (payload && payload.desc) ?? it.Description,
    Category:    (payload && (payload.category ?? payload.Category)) ?? it.Category,
    UoM:         (payload && payload.uom)  ?? it.UoM,
    Location:    (payload && payload.loc)  ?? it.Location,
    Status:      statusTo,
    UpdatedAt:   nowISO()
  });
  if (payload && Number(payload.price) > 0) {
    recordPrice_(payload.sku, Number(payload.price), 'MODIFY_SKU', 'Price adjusted', '', '');
  }

  const linkId = nextTrxId();
  const summary = `Modify ${payload.sku} — ${(payload.name || it.Name || '')}${payload.price ? ' ('+formatPHP_(payload.price)+')' : ''}`;
  _immediateLedger_('MODIFY_SKU', linkId, payload, summary, payload.sku, (payload.name || it.Name || ''), (payload.uom || it.UoM || ''), 0);

  const pseudoPen = { Type:'MODIFY_SKU', SKU:payload.sku, Name:(payload.name || it.Name || ''), UoM:(payload.uom||it.UoM||''), Qty:'', Delta:0, By:getCurrentUser().email, Note:summary, Reason:'', PendingID:'', LinkID:linkId, PayloadJSON: JSON.stringify(payload) };
  notifyApprovedEvent('MODIFY_SKU', pseudoPen);
  notifyRequesterResult('Approved', pseudoPen);
  return { ok:true, linkId };
}

function actionRetireSku(sku, note) {
  if (!sku) throw new Error('Missing SKU');
  const itemsSh = sheet(SHEET_ITEMS);
  const it = _findBy(itemsSh, 'SKU', sku); if (!it) throw new Error('SKU not found');
  if (Number(it.Qty || 0) !== 0) throw new Error('Cannot retire: stock must be exactly 0.');

  _updateByKey(itemsSh, 'SKU', sku, { Status:'Retired', UpdatedAt: nowISO() });

  const linkId = nextTrxId();
  const summary = note || `Retire ${sku}`;
  _immediateLedger_('RETIRE_SKU', linkId, { sku }, summary, sku, it.Name, it.UoM, 0);

  const pseudoPen = { Type:'RETIRE_SKU', SKU:sku, Name:it.Name, UoM:it.UoM, Qty:'', Delta:0, By:getCurrentUser().email, Note:summary, Reason:'', PendingID:'', LinkID:linkId };
  notifyApprovedEvent('RETIRE_SKU', pseudoPen);
  notifyRequesterResult('Approved', pseudoPen);
  return { ok:true, linkId };
}


function actionReceive(sku, qty, note, reactivateIfRetired) {
  if (!sku || !(qty > 0)) throw new Error('Invalid receive request');
  const me = getCurrentUser();
  const it = _findBy(sheet(SHEET_ITEMS), 'SKU', sku);
  if (!it) throw new Error('SKU not found');

  // Auto-approve (no Pending) when requester is a controller
  if (me.role === 'controller' && me.status === 'Active') {
    const itemsSh = sheet(SHEET_ITEMS);
    const fromQty = Number(it.Qty || 0);
    const toQty   = fromQty + Number(qty);
    const statusPatch = (it.Status === 'Retired' && toQty > 0) ? { Status:'Active' } : {};
    _updateByKey(itemsSh, 'SKU', sku, { Qty: toQty, UpdatedAt: nowISO(), ...statusPatch });

    const linkId = nextTrxId();
    const summary = note || `Receive ${qty} ${it.UoM} — ${sku}`;
    _append(sheet(SHEET_LEDGER), {
      ID: linkId, When: nowISO(), Type:'RECEIVE', SKU: sku, Item: it.Name,
      Delta: +qty, UoM: it.UoM, Status:'Approved', By: me.email,
      Note: appendNoteUnique_(summary, stamp_('Fully Approved', me.email, ''))
    });


    const pseudoPen = { Type:'RECEIVE', SKU:sku, Name:it.Name, UoM:it.UoM, Qty:qty, Delta:+qty, By:me.email, Note:summary, Reason:'', PendingID:'', LinkID:linkId, PayloadJSON: JSON.stringify(reactivateIfRetired ? { reactivateIfRetired:true } : null) };
    notifyApprovedEvent('RECEIVE', pseudoPen);
    notifyRequesterResult('Approved', pseudoPen);
    return { ok:true, linkId };
  }



  const payload = reactivateIfRetired ? { reactivateIfRetired: true } : null;
  return queuePending({
    type:'RECEIVE', sku, name:it.Name, uom:it.UoM, qty, delta:+qty,
    note: note || `Receive ${qty} ${it.UoM} — ${sku}`, payload
  });
}


function actionIssue(sku, qty, employee, department, reason, businessUnit, deploymentLocation) {
  if (!sku || !(qty > 0)) throw new Error('Invalid issue request');
  const me = getCurrentUser();
  const it = _findBy(sheet(SHEET_ITEMS), 'SKU', sku);
  if (!it) throw new Error('SKU not found');
  if (String(it.Status) !== 'Active') throw new Error('Item must be Active to issue.');
  if (Number(qty) > Number(it.Qty || 0)) throw new Error('Cannot issue more than on-hand quantity');

  const parenthetical = `${department}${businessUnit ? ' | ' + businessUnit : ''}`;
  const note = `Issued to ${employee} (${parenthetical}).`;

  // Auto-approve (no Pending) when requester is a controller
  if (me.role === 'controller' && me.status === 'Active') {
    const itemsSh = sheet(SHEET_ITEMS);
    const toQty = Number(it.Qty || 0) - Math.abs(qty);
    if (toQty < 0) throw new Error('Cannot issue more than on-hand during finalize.');
    _updateByKey(itemsSh, 'SKU', sku, { Qty: toQty, UpdatedAt: nowISO() });

    const linkId = nextTrxId();
    _append(sheet(SHEET_LEDGER), {
      ID: linkId, When: nowISO(), Type:'ISSUE', SKU: sku, Item: it.Name,
      Delta: -Math.abs(qty), UoM: it.UoM, Status:'Approved', By: me.email,
      Note: appendNoteUnique_(note, stamp_('Fully Approved', me.email, ''))
    });

    const payload = { meta: { employee:String(employee||''), department:String(department||''), businessUnit: businessUnit?String(businessUnit):'', deploymentLocation: deploymentLocation?String(deploymentLocation):'' } };
    const pseudoPen = { Type:'ISSUE', SKU:sku, Name:it.Name, UoM:it.UoM, Qty:qty, Delta:-Math.abs(qty), By:me.email, Note:note, Reason:String(reason||''), PendingID:'', LinkID:linkId, PayloadJSON: JSON.stringify(payload) };
    notifyApprovedEvent('ISSUE', pseudoPen);
    notifyRequesterResult('Approved', pseudoPen);
    return { ok:true, linkId };
  }


  return queuePending({
    type: 'ISSUE',
    sku,
    name: it.Name,
    uom: it.UoM,
    qty,
    delta: -Math.abs(qty),
    reason,
    note,
    payload: {
      meta: {
        employee: String(employee || ''),
        department: String(department || ''),
        businessUnit: businessUnit ? String(businessUnit) : '',
        deploymentLocation: deploymentLocation ? String(deploymentLocation) : ''
      }
    }
  });
}


function actionReceiveMulti(items, note) {
  if (!Array.isArray(items) || !items.length) throw new Error('No items to receive.');
  const me = getCurrentUser();
  const all = items.map(it => {
    const row = _findBy(sheet(SHEET_ITEMS), 'SKU', it.sku);
    if (!row) throw new Error('SKU not found: ' + it.sku);
    if (String(row.Status) !== 'Active') throw new Error('Item must be Active: ' + it.sku);
    const qty = Number(it.qty||0);
    if (!(qty > 0)) throw new Error('Invalid qty for ' + it.sku);
    return { SKU: row.SKU, Name: row.Name, UoM: row.UoM, qty, delta: +qty };
  });

  // Auto-approve path
  if (me.role === 'controller' && me.status === 'Active') {
    const itemsSh = sheet(SHEET_ITEMS);
    all.forEach(it => {
      const row = _findBy(itemsSh, 'SKU', it.SKU);
      const toQty = Number(row.Qty||0) + Number(it.qty||0);
      const statusPatch = (row.Status === 'Retired' && toQty > 0) ? { Status:'Active' } : {};
      _updateByKey(itemsSh, 'SKU', it.SKU, { Qty: toQty, UpdatedAt: nowISO(), ...statusPatch });
    });

    const linkId = nextTrxId();
    const sum = _summarizeMulti_('RECEIVE', all, note);
    _immediateLedger_('RECEIVE', linkId, { items: all, note: String(note||'') }, sum.listText, all.map(b=>b.SKU).join(', '), sum.title, 'mixed', all.reduce((a,b)=>a+Number(b.delta||0),0));

    const pseudoPen = { Type:'RECEIVE', SKU: all.map(b=>b.SKU).join(', '), Name: sum.title, UoM:'mixed', Qty:'', Delta: all.reduce((a,b)=>a+Number(b.delta||0),0), By: me.email, Note: sum.listText, Reason:'', PendingID:'', LinkID:linkId, PayloadJSON: JSON.stringify({items:all}) };
    notifyApprovedEvent('RECEIVE', pseudoPen);
    notifyRequesterResult('Approved', pseudoPen);
    return { ok:true, linkId };
  }

  return queuePendingMulti({ type:'RECEIVE', items: all, note: note||'' });
}


function actionIssueMulti(items, employee, department, reason, businessUnit, deploymentLocation) {
  if (!Array.isArray(items) || !items.length) throw new Error('No items to issue.');
  if (!employee)   throw new Error('Employee is required.');
  if (!department) throw new Error('Department is required.');
  if (!reason)     throw new Error('Reason is required.');

  const me = getCurrentUser();
  const all = items.map(it => {
    const row = _findBy(sheet(SHEET_ITEMS), 'SKU', it.sku);
    if (!row) throw new Error('SKU not found: ' + it.sku);
    if (String(row.Status) !== 'Active') throw new Error('Item must be Active: ' + it.sku);
    const qty = Number(it.qty||0);
    if (!(qty > 0)) throw new Error('Invalid qty for ' + it.sku);
    if (qty > Number(row.Qty||0)) throw new Error('Insufficient stock for ' + it.sku);
    return { SKU: row.SKU, Name: row.Name, UoM: row.UoM, qty, delta: -Math.abs(qty) };
  });

  const parenthetical = `${department}${businessUnit ? ' | ' + businessUnit : ''}`;
  const note = `Issued to ${employee} (${parenthetical}).`;

  // Auto-approve path
  if (me.role === 'controller' && me.status === 'Active') {
    const itemsSh = sheet(SHEET_ITEMS);
    all.forEach(it => {
      const row = _findBy(itemsSh, 'SKU', it.SKU);
      const toQty = Number(row.Qty||0) + Number(it.delta||0); // delta is negative
      if (toQty < 0) throw new Error('Cannot issue more than on-hand during finalize: ' + it.SKU);
      _updateByKey(itemsSh, 'SKU', it.SKU, { Qty: toQty, UpdatedAt: nowISO() });
    });

    const linkId = nextTrxId();
    const sum = _summarizeMulti_('ISSUE', all, note);
    _immediateLedger_('ISSUE', linkId, { items: all, note }, sum.listText, all.map(b=>b.SKU).join(', '), sum.title, 'mixed', all.reduce((a,b)=>a+Number(b.delta||0),0));

    const payload = { meta: { employee:String(employee||''), department:String(department||''), businessUnit: businessUnit?String(businessUnit):'', deploymentLocation: deploymentLocation?String(deploymentLocation):'' } };
    const pseudoPen = { Type:'ISSUE', SKU: all.map(b=>b.SKU).join(', '), Name: sum.title, UoM:'mixed', Qty:'', Delta: all.reduce((a,b)=>a+Number(b.delta||0),0), By: me.email, Note: sum.listText, Reason:String(reason||''), PendingID:'', LinkID:linkId, PayloadJSON: JSON.stringify({items:all, ...payload}) };
    notifyApprovedEvent('ISSUE', pseudoPen);
    notifyRequesterResult('Approved', pseudoPen);
    return { ok:true, linkId };
  }

  return queuePendingMulti({
    type: 'ISSUE',
    items: all,
    note,
    reason,
    meta: {
      employee: String(employee || ''),
      department: String(department || ''),
      businessUnit: businessUnit ? String(businessUnit) : '',
      deploymentLocation: deploymentLocation ? String(deploymentLocation) : ''
    }
  });
}




/* ---------------- Approvals ---------------- */
function approvePending(pendingId, commentOpt) {
  const me = getCurrentUser();
  if (!['controller','manager'].includes(me.role) || me.status !== 'Active') {
    throw new Error('Only controllers or managers can approve.');
  }
  const pen = _findBy(sheet(SHEET_PENDING), 'PendingID', pendingId);
  if (!pen) throw new Error('Request not found.');

  // NEW: requesters cannot approve their own requests (any role)
  if (String(pen.By || '').toLowerCase() === String(me.email || '').toLowerCase()) {
    throw new Error('Requesters cannot approve their own requests.');
  }


  // Enforce stage gate
  const stage     = Number(pen.Stage || 1);
  const nextRole  = String(pen.NextRole || '').trim();
  if (!nextRole) throw new Error('This request has no next approver role configured.');
  if (nextRole === 'controller' && me.role !== 'controller') throw new Error('Controller approval required for this step.');
  if (nextRole === 'manager'    && me.role !== 'manager')    throw new Error('Manager approval required for this step.');

  // Build/extend approval history
  const stampIso = nowISO();
  let hist = [];
  try { hist = pen.ApprovalsJSON ? JSON.parse(pen.ApprovalsJSON) : []; } catch(e){ hist = []; }
  hist.push({ step: stage, role: nextRole, by: me.email, at: stampIso, comment: String(commentOpt||'') });

  // Visible stamp into notes (emails, Pending, Ledger)
  const visStamp = stamp_('Approved', me.email, (commentOpt ? (' Comment: ' + String(commentOpt).trim()) : ''));
  const newPenNote = appendNoteUnique_(pen.Note || '', `[Stage ${stage}] ` + visStamp);
  _updateByKey(sheet(SHEET_PENDING), 'PendingID', pendingId, {
    Note: newPenNote,
    ApprovalsJSON: JSON.stringify(hist)
  });
  const led = _findBy(sheet(SHEET_LEDGER), 'ID', pen.LinkID);
  if (led) {
    const newLedNote = appendNoteUnique_(led.Note || '', `[Stage ${stage}] ` + visStamp);
    _updateByKey(sheet(SHEET_LEDGER), 'ID', pen.LinkID, { Note: newLedNote, ApprovalsJSON: JSON.stringify(hist) });
  }

  // Decide if this is FINAL step or needs escalation
  const type = pen.Type;
  const finalStep =
    (type === 'RECEIVE' && nextRole === 'controller') ||
    ((type === 'ISSUE' || type === 'REQUEST') && nextRole === 'controller' && (stage === 1 || stage === 2));

  if (!finalStep) {
    // move to next stage (ISSUE/REQUEST: manager -> controller)
    const next = ((type === 'ISSUE' || type === 'REQUEST') && nextRole === 'manager') ? 'controller' : '';
    if (!next) throw new Error('Flow configuration error: no next role.');
    _updateByKey(sheet(SHEET_PENDING), 'PendingID', pendingId, {
      Stage: stage + 1,
      NextRole: next
    });
    // re-notify next approvers with history included
    const updated = _findBy(sheet(SHEET_PENDING), 'PendingID', pendingId);
    notifyPendingCreated({
      linkId: updated.LinkID,
      pendingId: updated.PendingID,
      rec: { type: updated.Type, sku: updated.SKU, name: updated.Name, uom: updated.UoM, delta: updated.Delta, note: updated.Note },
      by: updated.By
    });
    return { ok:true, stage: stage + 1, nextRole: next };
  }

  // ===== FINAL approval: apply effect & close =====
  const itemsSh = sheet(SHEET_ITEMS);
  const payload = pen.PayloadJSON ? JSON.parse(pen.PayloadJSON) : null;

  // === Apply final effect to Items (RECEIVE / ISSUE) ===
  const itemsArr = (payload && Array.isArray(payload.items)) ? payload.items : null;

  function applyDeltaToSku(sku, delta, uomOpt) {
    const row = _findBy(itemsSh, 'SKU', sku);
    if (!row) throw new Error('SKU not found during finalize: ' + sku);

    const fromQty = Number(row.Qty || 0);
    const toQty   = fromQty + Number(delta || 0);

    if ((type === 'ISSUE' || type === 'REQUEST') && toQty < 0) {
      throw new Error('Cannot issue more than on-hand during finalize: ' + sku);
    }

    // Optional reactivation for single RECEIVE (from queuePending’s payload.reactivateIfRetired)
    let statusPatch = {};
    if (type === 'RECEIVE' && row.Status === 'Retired' && toQty > 0) {
      if (payload && payload.reactivateIfRetired) {
        statusPatch = { Status: 'Active' };
      } else {
        statusPatch = { Status: row.Status }; // no change
      }
    }

    _updateByKey(itemsSh, 'SKU', sku, {
      Qty: toQty,
      UoM: uomOpt || row.UoM,
      UpdatedAt: nowISO(),
      ...statusPatch
    });
  }

  if (itemsArr) {
    // Multi-item RECEIVE / ISSUE: deltas are already signed in each item.delta
    itemsArr.forEach(it => applyDeltaToSku(String(it.SKU), Number(it.delta || 0), it.UoM));
  } else {
    // Single-item RECEIVE / ISSUE: use pen row fields (Δ already signed)
    const deltaSigned = Number(pen.Delta || 0);
    applyDeltaToSku(String(pen.SKU), deltaSigned, pen.UoM);
  }

  // Mark Approved (final)
  // Pending remains "Approved"
  _updateByKey(sheet(SHEET_PENDING), 'PendingID', pendingId, {
    Status: 'Approved',
    ReviewedAt: stampIso,
    ReviewedBy: me.email
  });

  // Append "[Fully Approved]" to Note; keep Status at "Approved"
  const ledBefore = _findBy(sheet(SHEET_LEDGER), 'ID', pen.LinkID);
  const fullStamp = stamp_('Fully Approved', me.email, '');
  const newLedNoteFinal = appendNoteUnique_(ledBefore && ledBefore.Note ? ledBefore.Note : '', fullStamp);

  _updateByKey(sheet(SHEET_LEDGER),  'ID', pen.LinkID, {
    Status: 'Approved',
    ReviewedAt: stampIso,
    ReviewedBy: me.email,
    Note: newLedNoteFinal
  });


  const penNotify = _findBy(sheet(SHEET_PENDING), 'PendingID', pendingId);
  notifyApprovedEvent(type, penNotify);
  notifyRequesterResult('Approved', penNotify);
  return { ok:true, final:true };
  }

function declinePending(pendingId, reason) {
  const me = getCurrentUser();
  if (!['controller','manager'].includes(me.role) || me.status !== 'Active') {
    throw new Error('Only controllers or managers can decline.');
  }
  const pen = _findBy(sheet(SHEET_PENDING), 'PendingID', pendingId);
  if (!pen) throw new Error('Request not found.');

  // Requesters can’t act on their own requests
  if (String(pen.By || '').toLowerCase() === String(me.email || '').toLowerCase()) {
    throw new Error('Requesters cannot decline their own requests.');
  }
  if (String(pen.Status) !== 'Pending') {
    throw new Error('This request has already been processed (status: ' + pen.Status + ').');
  }

  // 🚦 Enforce the stage gate exactly like approvePending:
  const nextRole = String(pen.NextRole || '').trim().toLowerCase();
  if (!nextRole) throw new Error('This request has no next approver role configured.');
  if (nextRole === 'controller' && me.role !== 'controller') throw new Error('Controller decline required for this step.');
  if (nextRole === 'manager'    && me.role !== 'manager')    throw new Error('Manager decline required for this step.');

  const user       = me.email || 'unknown';
  const stampIso   = nowISO();
  const declineStamp = stamp_('Declined', user, ' — Reason: ' + String(reason).trim());

  // Set statuses on Pending + Ledger
  _updateByKey(sheet(SHEET_PENDING), 'PendingID', pendingId, {
    Status: 'Declined',
    Reason: String(reason).trim(),
    ReviewedAt: stampIso,
    ReviewedBy: user
  });

  // Append unique decline stamp to notes
  const penAfter   = _findBy(sheet(SHEET_PENDING), 'PendingID', pendingId);
  const newPenNote = appendNoteUnique_(penAfter.Note || '', declineStamp);
  _updateByKey(sheet(SHEET_PENDING), 'PendingID', pendingId, { Note: newPenNote });

  const led = _findBy(sheet(SHEET_LEDGER), 'ID', pen.LinkID);
  if (led) {
    const newLedNote = appendNoteUnique_(led.Note || '', declineStamp);
    _updateByKey(sheet(SHEET_LEDGER), 'ID', pen.LinkID, {
      Status: 'Declined',
      ReviewedAt: stampIso,
      ReviewedBy: user,
      Note: newLedNote
    });
  }

  // Record decline in ApprovalsJSON (history)
  let hist = [];
  try { hist = pen.ApprovalsJSON ? JSON.parse(pen.ApprovalsJSON) : []; } catch(e){ hist = []; }
  hist.push({
    step: Number(pen.Stage || 1),
    role: String(pen.NextRole || ''),
    by: user,
    at: stampIso,
    declined: true,
    reason: String(reason).trim()
  });
  _updateByKey(sheet(SHEET_PENDING), 'PendingID', pendingId, { ApprovalsJSON: JSON.stringify(hist) });
  const led2 = _findBy(sheet(SHEET_LEDGER), 'ID', pen.LinkID);
  if (led2) _updateByKey(sheet(SHEET_LEDGER), 'ID', pen.LinkID, { ApprovalsJSON: JSON.stringify(hist) });

  const updated = _findBy(sheet(SHEET_PENDING), 'PendingID', pendingId);
  notifyRequesterResult('Declined', updated);
  return { ok:true };
}


function backfillNextRoleOnPending() {
  const sh = sheet(SHEET_PENDING);
  ensureColumns(sh, ['Stage','NextRole','ApprovalsJSON']);  // just in case
  const rows = _readObjects(sh);
  rows
    .filter(r => String(r.Status) === 'Pending' && !String(r.NextRole || '').trim())
    .forEach(r => {
      const stage = Number(r.Stage || 1);
      const type  = String(r.Type || '').toUpperCase();
      const nextRole =
      (type === 'RECEIVE') ? 'controller' :
      ((type === 'ISSUE' || type === 'REQUEST') && stage === 1) ? 'manager' :
      ((type === 'ISSUE' || type === 'REQUEST') && stage >= 2) ? 'controller' :
      ''; // others shouldn't be in Pending
      if (nextRole) {
        _updateByKey(sh, 'PendingID', r.PendingID, { Stage: stage || 1, NextRole: nextRole });
      }
    });
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
  // Allow group tokens: u is OPTIONAL (only enforced later if present)
  if (!data || !data.pid || !data.a || !data.exp) throw new Error('Malformed token');
  if (Date.now() > Number(data.exp)) throw new Error('Token expired');
  return data; // { a: 'approve'|'decline', pid, u?, exp }
}

function makeActionLink_(action, pendingId, recipientEmailOpt, ttlMinutes) {
  var exp = Date.now() + (Math.max(1, ttlMinutes || (3*24*60))) * 60 * 1000; // default 3 days
  var u = recipientEmailOpt ? String(recipientEmailOpt).toLowerCase() : '';   // '' => generic (group)
  var payload = JSON.stringify({ a: action, pid: String(pendingId), u: u, exp: exp });
  var tok = _b64url_(payload) + '.' + _sign_(payload);
  var base = webAppUrl();
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

function _formatItemsTableHtml_(items) {
  // items: [{SKU, Name, UoM, qty, delta, price?}]  // we'll enrich from Items sheet for Description/Category/Price
  var itemsSheetMap = {};
  try {
    itemsSheetMap = Object.fromEntries(
      _readObjects(sheet(SHEET_ITEMS)).map(r => [String(r.SKU), r])
    );
  } catch(e){ itemsSheetMap = {}; }

  var rowsHtml = items.map(function(it){
    var sku = String(it.SKU || '');
    var itRow = itemsSheetMap[sku] || {};
    var desc = it.description || it.desc || itRow.Description || '';
    var cat  = it.category || it.Category || itRow.Category || '';
    var priceNum = (it.price != null ? Number(it.price) : (it.unitPrice != null ? Number(it.unitPrice) : Number(itRow.UnitPrice || 0)));
    var priceDisp = (priceNum && priceNum > 0) ? formatPHP_(priceNum) : '';

    return '' +
      '<tr>' +
        '<td style="padding:8px 10px;border:1px solid #e5e7eb">'+safe(sku)+'</td>' +
        '<td style="padding:8px 10px;border:1px solid #e5e7eb">'+safe(it.Name)+'</td>' +
        '<td style="padding:8px 10px;border:1px solid #e5e7eb">'+safe(desc)+'</td>' +
        '<td style="padding:8px 10px;border:1px solid #e5e7eb">'+safe(cat)+'</td>' +
        '<td style="padding:8px 10px;border:1px solid #e5e7eb;text-align:right"><b>'+safe(it.qty)+'</b></td>' +
        '<td style="padding:8px 10px;border:1px solid #e5e7eb">'+safe(it.UoM)+'</td>' +
        '<td style="padding:8px 10px;border:1px solid #e5e7eb;text-align:right">'+priceDisp+'</td>' +
      '</tr>';
  }).join('');

  return '' +
    '<div style="margin-top:12px;font-weight:700">Items ('+items.length+')</div>' +
    '<table role="presentation" width="100%" style="border-collapse:collapse;margin-top:6px">' +
      '<thead><tr>' +
        '<th style="padding:8px 10px;border:1px solid #e5e7eb;background:#f9fafb;text-align:left;color:#6b7280">SKU</th>' +
        '<th style="padding:8px 10px;border:1px solid #e5e7eb;background:#f9fafb;text-align:left;color:#6b7280">Item</th>' +
        '<th style="padding:8px 10px;border:1px solid #e5e7eb;background:#f9fafb;text-align:left;color:#6b7280">Description</th>' +
        '<th style="padding:8px 10px;border:1px solid #e5e7eb;background:#f9fafb;text-align:left;color:#6b7280">Category</th>' +
        '<th style="padding:8px 10px;border:1px solid #e5e7eb;background:#f9fafb;text-align:right;color:#6b7280">Qty</th>' +
        '<th style="padding:8px 10px;border:1px solid #e5e7eb;background:#f9fafb;text-align:left;color:#6b7280">UoM</th>' +
        '<th style="padding:8px 10px;border:1px solid #e5e7eb;background:#f9fafb;text-align:right;color:#6b7280">Price</th>' +
      '</tr></thead>' +
      '<tbody>' + rowsHtml + '</tbody>' +
    '</table>';
}



function _summarizeMulti_(type, items, note) {
  var n = items.length;
  var t = (type === 'RECEIVE') ? 'Receive'
        : (type === 'ISSUE')   ? 'Issue'
        : (type === 'CREATE_SKU') ? 'Create SKU'
        : type;

  var lines = items.map(function(it, idx){
    // read with casing fallbacks
    var sku = (it.SKU != null ? it.SKU : it.sku);
    var name = (it.Name != null ? it.Name : it.name);
    var uom  = (it.UoM  != null ? it.UoM  : it.uom);
    var qty  = (it.qty  != null ? it.qty  : it.quantity);
    var price = (it.price != null ? it.price : it.unitPrice);

    var priceBit = (price && Number(price) > 0) ? (' · ' + formatPHP_(price)) : '';

    if (type === 'CREATE_SKU') {
      // IMPORTANT: emit the same "<i>. <qty> <uom> — <name> (<SKU>)" pattern
      // to satisfy your frontend/regex parser (qty is 0 for create)
      var skuDisp = safe(sku || '(auto)');
      var u = safe(uom || 'mixed');  // always have a token
      var nm = safe(name || '(no name)');
      return (idx+1)+'. 0 ' + u + ' — ' + nm + ' (' + skuDisp + ')' + priceBit;
    }

    // RECEIVE / ISSUE: keep existing shape with fallbacks
    return (idx+1)+'. ' + safe(qty) + ' ' + safe(uom) + ' — ' + safe(name) + ' (' + safe(sku) + ')' + priceBit;
  }).join('\n');

  var head = t + ' — ' + n + ' item(s)';
  return { title: head, listText: lines };
}




function _buildApprovalNote_(penRow, approverCommentOpt) {
  // Try to use payload items first (multi), otherwise fallback to single row fields
  var items = [];
  try {
    var p = penRow && penRow.PayloadJSON ? JSON.parse(penRow.PayloadJSON) : null;
    if (p && Array.isArray(p.items) && p.items.length) {
      items = p.items.map(function (it, i) {
        return (i + 1) + '. ' + safe(it.qty) + ' ' + safe(it.UoM) + ' — ' + safe(it.Name) + ' (' + safe(it.SKU) + ')';
      });
    }
  } catch (e) {}

  if (!items.length && penRow && penRow.Name) {
    items = ['1. ' + safe(penRow.Qty) + ' ' + safe(penRow.UoM) + ' — ' + safe(penRow.Name) + ' (' + safe(penRow.SKU) + ')'];
  }

  var blocks = [];
  if (items.length) blocks.push(items.join('\n'));

  // Requester-facing details (Reason/Notes/Remarks)
  var requesterBits = [];
  try {
    var p2 = penRow && penRow.PayloadJSON ? JSON.parse(penRow.PayloadJSON) : null;
    if (p2 && p2.reason) requesterBits.push('Reason: ' + String(p2.reason));
    if (p2 && p2.note)   requesterBits.push('Note: '   + String(p2.note));
  } catch (e) {}

  if (!requesterBits.length) {
    if (penRow.Reason) requesterBits.push('Reason: ' + String(penRow.Reason));
    // "Note / Remarks" from the user request lives here
    if (penRow.Note)   requesterBits.push('Note: '   + String(penRow.Note));
  }
  if (requesterBits.length) blocks.push(requesterBits.join(' | '));

  // Approver comment
  var c = (approverCommentOpt && String(approverCommentOpt).trim()) ? String(approverCommentOpt).trim() : '';
  if (c) blocks.push('Comment: ' + c);

  return blocks.join('\n');
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

    // Must be signed in; if token carries a specific 'u', enforce match
    var me = getCurrentUser();
    if (!me.email) return renderActionPage_('Sign-in required', 'Please sign in with your YDC account, then click the link again.', 'info');

    if (data.u) { // personalized token
      if (String(me.email).toLowerCase() !== String(data.u).toLowerCase()) {
        var switchLink = 'https://accounts.google.com/Logout';
        return renderActionPage_(
          'Wrong Google Account',
          'This link was issued to <b>' + data.u + '</b> but you are signed in as <b>' + me.email + '</b>.<br>' +
          'Please <a href="'+switchLink+'" target="_blank" rel="noopener">switch accounts</a> and try again.',
          'warning'
        );
      }
    }


    // Must be controller or manager
    if (!( ['controller','manager'].includes(String(me.role)) && String(me.status) === 'Active')) {
      return renderActionPage_('Not authorized',
        'Only Controllers or Managers with Active status can approve/decline. Your role: ' + (me.role || '—') + '.', 'danger');
    }

    // === NEW: show "already processed" page instead of forms ===
    var pen = _findBy(sheet(SHEET_PENDING), 'PendingID', data.pid);
    if (!pen) {
      return renderActionPage_('Request not found', 'This request no longer exists (it may have been archived).', 'warning');
    }

    // NEW: requesters cannot act on their own requests (no approve/decline)
    if (String(pen.By || '').toLowerCase() === String(me.email || '').toLowerCase()) {
      return renderActionPage_('Not allowed', 'Requesters cannot approve or decline their own requests.', 'danger', data.pid);
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
  '<div style="font-weight:800;font-size:22px;margin-bottom:8px;color:#111">Confirm Decision — Approve</div>' +
  '<div style="color:#374151">Pending ID: <b>'+pendingId+'</b></div>' +
  '<label style="display:block;margin-top:12px;font-weight:600">Comment (optional)</label>' +
  '<textarea id="comment" rows="3" placeholder="Add a note for the requester/ledger (optional)" ' +
           'style="width:100%;padding:10px;border:1px solid #d1d5db;border-radius:10px"></textarea>' +
  '<div style="margin-top:14px;text-align:center">' +
    '<button id="btnApprove" style="padding:10px 16px;border-radius:8px;background:#16a34a;color:#fff;border:0;font-weight:700">Confirm Approve</button>' +
    '<a href="'+webAppUrl()+'" style="display:inline-block;margin-left:8px;padding:10px 16px;border-radius:8px;background:#0d6efd;color:#fff;text-decoration:none;font-weight:700">Open Web App</a>' +
    '<span id="spin" style="display:none;margin-left:8px;width:14px;height:14px;border:2px solid #d1d5db;border-top-color:#0d6efd;border-radius:50%;vertical-align:middle;animation:spin .8s linear infinite"></span>' +
  '</div>' +
  '<div id="msg" style="margin-top:12px;color:#374151;text-align:center"></div>' +
'</div>' +
'<script>' +
'  (function(){' +
'    var btn = document.getElementById("btnApprove");' +
'    var msg = document.getElementById("msg");' +
'    var spin = document.getElementById("spin");' +
'    btn.addEventListener("click", function(){' +
'      var c = (document.getElementById("comment")?.value || "").trim();' +
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
'        .approvePending("'+pendingId+'", c);' +
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
  '<div style="font-weight:800;font-size:22px;margin-bottom:8px;color:#111">Confirm Decision — Decline</div>' +
  '<div style="color:#374151">Pending ID: <b>'+pendingId+'</b></div>' +
  '<label style="display:block;margin-top:12px;font-weight:600">Reason (required)</label>' +
  '<textarea id="reason" rows="3" style="width:100%;padding:10px;border:1px solid #d1d5db;border-radius:10px"></textarea>' +
  '<div style="margin-top:14px;text-align:center">' +
    '<button id="btnGo" style="padding:10px 16px;border-radius:8px;background:#a61b29;color:#fff;border:0;font-weight:700">Confirm Decline</button>' +
    '<a href="'+webAppUrl()+'" style="display:inline-block;margin-left:8px;padding:10px 16px;border-radius:8px;background:#0d6efd;color:#fff;text-decoration:none;font-weight:700">Open Web App</a>' +
    '<span id="spin" style="display:none;margin-left:8px;width:14px;height:14px;border:2px solid #d1d5db;border-top-color:#0d6efd;border-radius:50%;vertical-align:middle;animation:spin .8s linear infinite"></span>' +
  '</div>' +
  '<div id="msg" style="margin-top:12px;color:#374151;text-align:center"></div>' +
'</div>' +
'<script>' +
'  (function(){' +
'    var btn = document.getElementById("btnGo");' +
'    var msg = document.getElementById("msg");' +
'    var spin = document.getElementById("spin");' +
'    btn.addEventListener("click", function(){' +
'      var r = (document.getElementById("reason")?.value || "").trim();' +
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

  const user       = me.email || 'unknown';
  const stampIso   = nowISO();
  const stampLocal = new Date().toLocaleString();

  const voidStamp = stamp_('Voided', user, ' — Reason: ' + String(reason).trim());

  _updateByKey(sheet(SHEET_PENDING), 'PendingID', pendingId, {
    Status: 'Voided',
    Reason: String(reason).trim(),
    ReviewedAt: stampIso,
    ReviewedBy: user
  });

  const penAfter = _findBy(sheet(SHEET_PENDING), 'PendingID', pendingId);
  const newPenNote = appendNoteUnique_(penAfter.Note || '', voidStamp);
  _updateByKey(sheet(SHEET_PENDING), 'PendingID', pendingId, { Note: newPenNote });

  const led = _findBy(sheet(SHEET_LEDGER), 'ID', pen.LinkID);
  if (led) {
    const newLedNote = appendNoteUnique_(led.Note || '', voidStamp);
    _updateByKey(sheet(SHEET_LEDGER), 'ID', pen.LinkID, {
      Status: 'Voided',
      ReviewedAt: stampIso,
      ReviewedBy: user,
      Note: newLedNote
    });
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
function getManagers() {
  return _readObjects(sheet(SHEET_USERS))
    .filter(u => String(u.Role) === 'manager' && String(u.Status) === 'Active')
    .map(u => String(u.Email || '').trim())
    .filter(Boolean);
}
function resolveRoleRecipients(role){
  if (role === 'controller') return { to: getControllers(), cc: [] };
  if (role === 'manager')    return { to: getManagers(), cc: [] };
  return { to: [], cc: [] };
}
function resolveRecipients(eventKey, opts) {
  if (opts && opts.role) return resolveRoleRecipients(opts.role); // ⬅ NEW
  if (opts && opts.controllersOnly) {                             // legacy callers
    return { to: getApprovers(), cc: [] };
  }
  const cfg = readNotifyConfig()[eventKey] || { enabled:true, recipients:[], cc:[] };
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
function formatPHP_(n) {
  var x = Number(n);
  if (!(x > 0)) return '';
  return '₱' + x.toFixed(2);
}

function friendlyType(t){
  return ({
    CREATE_SKU:'Create SKU',
    MODIFY_SKU:'Modify Item',
    RETIRE_SKU:'Retire SKU',
    RECEIVE:'Receive (Inbound)',
    ISSUE:'Issue (Outbound)',
    REQUEST:'Request Item',           // ⬅️ new
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

  var extraBelow = (opts && opts.extraBelow) ? String(opts.extraBelow) : '';

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
                extraBelow +           /* ⬅️ items table injected here (actions still at the very end) */
                actionsHtml +
              '</td></tr>' +
            '</table>' +
          '</td></tr>' +
        '</table>' +
      '</td></tr>' +
    '</table>' +
  '</center>';
}

function renderHistoryBlock_(penRow){
  try{
    const hist = penRow && penRow.ApprovalsJSON ? JSON.parse(penRow.ApprovalsJSON) : [];
    if (!hist.length) return '';
    const rows = hist.map(h => {
      const tag = h.declined ? 'Declined' : 'Approved';
      const when = h.at ? new Date(h.at).toLocaleString() : '—';
      const commentBit = h.comment ? (' · ' + safe(h.comment)) : '';
      const reasonBit  = h.reason  ? (' · Reason: ' + safe(h.reason)) : '';
      return `<li>${tag} — Step ${safe(h.step)} (${safe(h.role)}) by ${safe(h.by)} @ ${when}${commentBit}${reasonBit}</li>`;
    }).join('');
    return `<div style="margin-top:10px;font-weight:700">Previous Decisions</div><ul style="margin-top:6px">${rows}</ul>`;
  }catch(e){ return ''; }
}


/* ---------- Per-event notifiers ---------- */

// Approval queued: controllers only + requester gets a separate email
function notifyPendingCreated(ctx){
  var r = ctx.rec;
  var typeNice = friendlyType(r.type);

  // Look up the Pending row to get NextRole and payload
  var pen = _findBy(sheet(SHEET_PENDING), 'PendingID', ctx.pendingId);

  // Detect multi from LinkID (read back payload items)
  var items = [];
  try {
    var p = pen && pen.PayloadJSON ? JSON.parse(pen.PayloadJSON) : null;
    items = (p && p.items) || [];
  } catch(e){}

  // If this is a single REQUEST (or ISSUE) and no items array, synthesize one for the email table
  if ((!items || !items.length) && (String(pen.Type) === 'REQUEST' || String(pen.Type) === 'ISSUE')) {
    items = [{
      SKU: r.sku, Name: r.name, UoM: r.uom,
      qty: Math.abs(Number(r.delta || 0)),
      delta: Number(r.delta || 0)
    }];
  }

  // Build extra items table — enriched for Desc/Category/Price
  var extra = (items && items.length) ? _formatItemsTableHtml_(items) : '';

  // History block (under the items table or alone)
  var historyHtml = renderHistoryBlock_(pen);
  var extraWithHistory = (extra || '') + historyHtml;

  // Single-item enrichment (Description/Category/Price)
  var singleDesc = '', singleCat = '', singlePriceRow = null;
  if (!items.length) {
    try {
      var itRow = _findBy(sheet(SHEET_ITEMS), 'SKU', r.sku);
      singleDesc = itRow ? String(itRow.Description || '') : '';
      singleCat  = itRow ? String(itRow.Category || '') : '';
      // price can come from payload.price or Items.UnitPrice
      var p2 = pen && pen.PayloadJSON ? JSON.parse(pen.PayloadJSON) : null;
      var priceNum = (p2 && Number(p2.price) > 0) ? Number(p2.price)
                    : (itRow && Number(itRow.UnitPrice) > 0 ? Number(itRow.UnitPrice) : 0);
      singlePriceRow = (priceNum > 0) ? ['Unit Price', formatPHP_(priceNum)] : null;
    } catch(e){}
  }

  var baseRows = [
    ['Type', typeNice],
    ['SKU', safe(r.sku)],
    !items.length ? ['Item', safe(r.name)] : null,
    !items.length ? ['Description', safe(singleDesc)] : null,
    !items.length ? ['Category', safe(singleCat)] : null,
    singlePriceRow,
    !items.length ? ['Quantity Δ', safe(r.delta)] : null,
    ['Requested By', safe(ctx.by)],
    ['Note', safe(r.note)],
    ['Pending ID', safe(ctx.pendingId)],
    ['Ledger ID', safe(ctx.linkId)]
  ].filter(Boolean);

  var subjectSfx = items.length ? (' — ' + items.length + ' item(s)') : (r.sku ? (' — ' + r.sku) : '');

  // Approver role for this stage
  var approverRole = String(pen && pen.NextRole || '').trim().toLowerCase() || 'controller';
  var recips = (approverRole === 'manager') ? getManagers() : getControllers();
  var requesterEmailLc = String(ctx.by || '').toLowerCase();
  recips = recips.map(String).filter(Boolean).filter(function(e){ return e.toLowerCase() !== requesterEmailLc; }).filter(function(v,i,a){return a.indexOf(v)===i;});

  if (recips.length){
    // ONE group email; links are generic (not bound to a specific email)
    var approveUrl = makeActionLink_('approve', ctx.pendingId /* generic */);
    var declineUrl = makeActionLink_('decline', ctx.pendingId /* generic */);

    var html = cardEmail(
      'Approval Needed — ' + typeNice,
      [['Current Approver', approverRole === 'manager' ? 'Managers' : 'Controllers']].concat(baseRows),
      {
        subtitle: 'Anyone on this list may approve directly from this email.',
        extraBelow: extraWithHistory,
        buttons: [
          { text:'Approve', href: approveUrl, bg:'#16a34a' },
          { text:'Decline', href: declineUrl, bg:'#dc2626' },
          { text:'Open Web App', href: webAppUrl(), bg:'#0d6efd' }
        ]
      }
    );
    sendMailSafe(recips, ('[Approval Needed] ' + typeNice + subjectSfx).trim(), html, []);
  }

  // Requester copy (no duplicate "Current Approver")
  if (ctx.by && /@/.test(ctx.by)) {
    var html2 = cardEmail(
      'Submitted for Approval — ' + typeNice,
      [['Status','Pending'], ['Current Approver', approverRole === 'manager' ? 'Managers' : 'Controllers']].concat(baseRows),
      { subtitle:'Your request has been queued and is awaiting approval.', extraBelow: extraWithHistory }
    );
    sendMailSafe([ctx.by], ('[Submitted] ' + typeNice + subjectSfx).trim(), html2, []);
  }
}


// Approved event (goes to configured recipients for that type)
function notifyApprovedEvent(type, penRow){
  const key = ({
    RECEIVE:NE.RECEIVE,
    ISSUE:NE.ISSUE,
    REQUEST:NE.ISSUE,          // reuse same recipients as Issue
    CREATE_SKU:NE.CREATE_SKU,
    MODIFY_SKU:NE.MODIFY_SKU,
    RETIRE_SKU:NE.RETIRE_SKU
  })[type];
  if (!key) return;
  const { to, cc } = resolveRecipients(key, {});
  if (!to.length) return;

  const typeNice = friendlyType(type);

  // Multi vs single layout
  let isMulti = false;
  let extra = '';
  let rows;

  try {
    const p = penRow && penRow.PayloadJSON ? JSON.parse(penRow.PayloadJSON) : null;
    const items = (p && Array.isArray(p.items)) ? p.items : [];
    if (items.length) {
      isMulti = true;
      // enriched table includes Desc/Category/Price (with UnitPrice fallback)
      extra = _formatItemsTableHtml_(items);
      const totalQty   = items.reduce((a,b)=> a + Number(b.qty || 0), 0);
      const totalDelta = items.reduce((a,b)=> a + Number(b.delta || 0), 0);
      const skus       = items.map(it => String(it.SKU || '')).filter(Boolean);
      const namesList  = items.map(it => String(it.Name || it.name || it.SKU || '')).filter(Boolean).join(', ');
      const uoms       = Array.from(new Set(items.map(it => String(it.UoM || '')).filter(Boolean)));
      const uomCell    = (uoms.length === 1) ? uoms[0] : 'mixed';

      rows = [
        ['Type', typeNice],
        ['SKU', safe(skus.join(', '))],
        ['Items', safe(namesList)],                 // ← list names instead of "n item(s)"
        ['UoM', uomCell],
        ['Quantity', String(totalQty)],
        ['Δ', String(totalDelta)],
        ['Requested By', safe(penRow.By)],
        ['Note', safe(penRow.Note)],
        ['Pending ID', safe(penRow.PendingID)],
        ['Ledger ID', safe(penRow.LinkID)]
      ];
    }
  } catch(e){}

  if (!isMulti) {
    // For REQUEST (and ISSUE) single, synthesize a one-row items table so details are tabular
    if (String(penRow.Type) === 'REQUEST' || String(penRow.Type) === 'ISSUE') {
      const singleItems = [{
        SKU: penRow.SKU, Name: penRow.Name, UoM: penRow.UoM,
        qty: Math.abs(Number(penRow.Qty || 0)),
        delta: Number(penRow.Delta || 0)
      }];
      extra = _formatItemsTableHtml_(singleItems);
    }
    // Enrich single with Description/Category/Price
    let priceRow = null, descRow = null, catRow = null;
    try {
      const p = penRow && penRow.PayloadJSON ? JSON.parse(penRow.PayloadJSON) : null;
      const itRow = _findBy(sheet(SHEET_ITEMS), 'SKU', penRow.SKU);
      const price = (p && Number(p.price) > 0) ? Number(p.price)
                   : (itRow && Number(itRow.UnitPrice) > 0 ? Number(itRow.UnitPrice) : 0);
      if (price > 0) priceRow = ['Unit Price', formatPHP_(price)];
      if (itRow) {
        descRow = ['Description', safe(itRow.Description)];
        catRow  = ['Category', safe(itRow.Category)];
      }
    } catch(e){}
    rows = [
      ['SKU', safe(penRow.SKU)],
      ['Item', safe(penRow.Name)],
      descRow, catRow,
      ['UoM', safe(penRow.UoM)],
      priceRow,
      ['Quantity', safe(penRow.Qty)],
      ['Δ', safe(penRow.Delta)],
      ['Requested By', safe(penRow.By)],
      ['Note', safe(penRow.Note)],
      ['Pending ID', safe(penRow.PendingID)],
      ['Ledger ID', safe(penRow.LinkID)]
    ].filter(Boolean);
  }

  const historyHtml = renderHistoryBlock_(penRow);
  const extraWithHistory = (extra || '') + historyHtml;

  const html = cardEmail(`Approved — ${typeNice}`, rows, { subtitle: 'Your request has been approved.', extraBelow: extraWithHistory });
  sendMailSafe(to, `[Approved] ${typeNice} ${penRow.SKU || ''}`.trim(), html, cc);
}


// Requester result (Approved/Declined/Voided) — subtitle reflects who acted
function notifyRequesterResult(result, penRow){
  const to = (penRow.By && /@/.test(penRow.By)) ? [penRow.By] : [];
  if (!to.length) return;

  const typeNice = friendlyType(penRow.Type);

  // Work out who performed the final action (for "Voided" phrasing)
  const actorEmail = String(penRow.ReviewedBy || '').trim();
  const requesterEmail = String(penRow.By || '').trim();
  const isSelfAction = actorEmail && requesterEmail &&
    actorEmail.toLowerCase() === requesterEmail.toLowerCase();
  const actorRole = actorEmail ? getRoleByEmail(actorEmail) : '';

  let subtitle =
    result === 'Approved' ? 'Your request has been approved.' :
    result === 'Declined' ? 'Your request has been declined.' :
    result === 'Voided'
      ? (isSelfAction
          ? 'You canceled this request.'
          : (actorRole === 'controller'
              ? 'Your request was voided (removed from the queue) by a controller.'
              : (actorRole === 'manager'
                  ? 'Your request was voided (removed from the queue) by a manager.'
                  : 'Your request was voided (removed from the queue).')))
      : '';

  let isMulti = false;
  let extra = '';
  let rows;

  try {
    const p = penRow && penRow.PayloadJSON ? JSON.parse(penRow.PayloadJSON) : null;
    const items = (p && Array.isArray(p.items)) ? p.items : [];
    if (items.length) {
      isMulti = true;
      // enriched multi table (Description/Category/Price)
      extra = _formatItemsTableHtml_(items);
      const totalQty   = items.reduce((a,b)=> a + Number(b.qty || 0), 0);
      const totalDelta = items.reduce((a,b)=> a + Number(b.delta || 0), 0);
      const skus       = items.map(it => String(it.SKU || '')).filter(Boolean);
      const namesList  = items.map(it => String(it.Name || it.name || it.SKU || '')).filter(Boolean).join(', ');
      const uoms       = Array.from(new Set(items.map(it => String(it.UoM || '')).filter(Boolean)));
      const uomCell    = (uoms.length === 1) ? uoms[0] : 'mixed';

      rows = [
        ['Type', typeNice],
        ['SKU', safe(skus.join(', '))],
        ['Items', safe(namesList)],
        ['UoM', uomCell],
        ['Quantity', String(totalQty)],
        ['Δ', String(totalDelta)],
        ['Note', safe(penRow.Note)],
        ['Reason', safe(penRow.Reason)],
        ['Pending ID', safe(penRow.PendingID)],
        ['Ledger ID', safe(penRow.LinkID)]
      ];
    }
  } catch(e){}

  if (!isMulti) {
    // For REQUEST/ISSUE single, synthesize a one-row items table so details are tabular
    if (String(penRow.Type) === 'REQUEST' || String(penRow.Type) === 'ISSUE') {
      const singleItems = [{
        SKU: penRow.SKU, Name: penRow.Name, UoM: penRow.UoM,
        qty: Math.abs(Number(penRow.Qty || 0)),
        delta: Number(penRow.Delta || 0)
      }];
      extra = _formatItemsTableHtml_(singleItems);
    }
    // Enrich single with Description/Category/Price
    let priceRow = null, descRow = null, catRow = null;
    try {
      const p = penRow && penRow.PayloadJSON ? JSON.parse(penRow.PayloadJSON) : null;
      const itRow = _findBy(sheet(SHEET_ITEMS), 'SKU', penRow.SKU);
      const price = (p && Number(p.price) > 0) ? Number(p.price)
                   : (itRow && Number(itRow.UnitPrice) > 0 ? Number(itRow.UnitPrice) : 0);
      if (price > 0) priceRow = ['Unit Price', formatPHP_(price)];
      if (itRow) {
        descRow = ['Description', safe(itRow.Description)];
        catRow  = ['Category', safe(itRow.Category)];
      }
    } catch(e){}
    rows = [
      ['Type', typeNice],
      ['SKU', safe(penRow.SKU)],
      ['Item', safe(penRow.Name)],
      descRow, catRow,
      ['UoM', safe(penRow.UoM)],
      priceRow,
      ['Quantity', safe(penRow.Qty)],
      ['Δ', safe(penRow.Delta)],
      ['Note', safe(penRow.Note)],
      ['Reason', safe(penRow.Reason)],
      ['Pending ID', safe(penRow.PendingID)],
      ['Ledger ID', safe(penRow.LinkID)]
    ].filter(Boolean);
  }

  const historyHtml = renderHistoryBlock_(penRow);
  const extraWithHistory = (extra || '') + historyHtml;

  const html = cardEmail(`${result} — ${typeNice}`, rows, { subtitle, extraBelow: extraWithHistory });
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
