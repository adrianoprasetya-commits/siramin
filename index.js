// SIRAMIN — one serverless function that runs the whole auction backend on Vercel.
// Single source of truth = Upstash Redis (key "siramin:db"). Every owner/consumer screen reads it through this API.
// Emails go out through Gmail SMTP (nodemailer). Secrets only come from environment variables, never from the browser.
const crypto = require('crypto');

const KEY = 'siramin:db', LOCK = 'siramin:lock';
const env = n => process.env[n] || '';
const RURL = () => env('KV_REST_API_URL') || env('UPSTASH_REDIS_REST_URL');
const RTOK = () => env('KV_REST_API_TOKEN') || env('UPSTASH_REDIS_REST_TOKEN');
const OWNER_USER = () => (env('OWNER_USERNAME') || 'SiramOfficial').toLowerCase();
const OWNER_EMAIL = () => env('OWNER_EMAIL') || env('EMAIL_USER') || 'SiraminOfficial@gmail.com';
const SITE = () => env('SITE_URL') || (env('VERCEL_PROJECT_PRODUCTION_URL') ? 'https://' + env('VERCEL_PROJECT_PRODUCTION_URL') : '');
const sleep = ms => new Promise(r => setTimeout(r, ms));
const nowMs = () => Date.now();

// ---------------- Redis (Upstash REST) ----------------
async function rc(...cmd) {
  if (!RURL() || !RTOK()) throw new Error('Database belum terpasang (variabel KV_REST_API_URL / KV_REST_API_TOKEN kosong)');
  const r = await fetch(RURL(), { method: 'POST', headers: { Authorization: 'Bearer ' + RTOK(), 'Content-Type': 'application/json' }, body: JSON.stringify(cmd) });
  const j = await r.json();
  if (j.error) throw new Error(j.error);
  return j.result;
}
const fresh = () => ({ status: 'idle', start: 699000, inc: 1000, stock: 1, sched: null, durMin: 10, startedAt: null, startedBy: null, endsAt: null,
  bids: [], out: [], round: 1, history: [], followers: [], outbox: [], notifs: [], orders: [], rev: 0 });
async function load() { const raw = await rc('GET', KEY); return raw ? JSON.parse(raw) : { auction: fresh(), users: {} }; }
let cache = { t: 0, db: null };
async function readCached(force) { if (!force && cache.db && nowMs() - cache.t < 1000) return cache.db; cache = { t: nowMs(), db: await load() }; return cache.db; }

// Everything that changes data runs inside this lock (read -> change -> write), so two simultaneous bids can never overwrite each other.
async function locked(fn) {
  const id = crypto.randomBytes(6).toString('hex');
  for (let i = 0; i < 50; i++) {
    if (await rc('SET', LOCK, id, 'NX', 'PX', 8000)) {
      try {
        const db = await load(); const out = await fn(db);
        if (!db.__nochange) { db.auction.rev++; await rc('SET', KEY, JSON.stringify(db)); cache = { t: nowMs(), db }; }
        return out;
      } finally { try { if ((await rc('GET', LOCK)) === id) await rc('DEL', LOCK); } catch (e) {} }
    }
    await sleep(60);
  }
  throw new Error('Server sedang sibuk, coba lagi.');
}

// ---------------- helpers ----------------
const fmt = n => 'Rp' + Number(n).toLocaleString('id-ID');
const fdt = t => { const d = new Date(t); return new Intl.DateTimeFormat('id-ID', { dateStyle: 'long', timeZone: 'Asia/Jakarta' }).format(d) + ' pukul ' + new Intl.DateTimeFormat('en-GB', { hour: '2-digit', minute: '2-digit', hour12: false, timeZone: 'Asia/Jakarta' }).format(d) + ' WIB'; };
const aid = a => 'AUC-' + String(a.round).padStart(3, '0');
const schedMs = a => (a.sched ? Date.parse(a.sched + ':00+07:00') : null);   // schedule is always WIB (UTC+7)
const win = a => [...a.bids].reverse().find(b => !a.out.includes(b.name)) || null;
const esc = s => String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const notify = (a, to, text) => { a.notifs.push({ id: crypto.randomUUID(), to, text, t: nowMs() }); a.notifs = a.notifs.slice(-200); };
const userByName = (db, name) => Object.values(db.users).find(z => z.name === name);
const hasOrder = a => a.orders.some(o => o.round === a.round && (o.status === 'awaiting' || o.status === 'paid'));

// ---------------- emails (stored as small recipes, HTML is built when sending) ----------------
function queue(db, to, subject, kind, data) {
  const a = db.auction; if (!to) return;
  a.outbox.push({ id: crypto.randomBytes(5).toString('hex'), to, subject, kind, data: data || {}, status: 'pending', tries: 0, t: nowMs() });
  a.outbox = a.outbox.slice(-80);
}
function recipe(kind, d) {
  const P = (...x) => x, p = d.product || 'Irigasi Otomatis Pintar';
  switch (kind) {
    case 'confirm': return { title: 'Anda mengikuti Lelang SIRAMIN', paras: P(`Halo <b>${esc(d.name)}</b>, Anda sekarang mengikuti lelang <b>${esc(p)}</b> (${esc(d.aid)}).`, 'Lelang <b>belum dimulai</b>. ' + (d.sched ? `Jadwal mulai: <b>${esc(fdt(Date.parse(d.sched + ':00+07:00')))}</b>.` : 'Pemilik belum menjadwalkan lelang.'), `Harga awal <b>${fmt(d.start)}</b> · durasi lelang <b>${esc(d.dur)}</b>.`, 'Kami akan mengirim pengingat 10 menit sebelum mulai dan saat lelang LIVE.'), cta: ['Lihat Lelang'] };
    case 'schedule': return { title: 'Jadwal Lelang SIRAMIN diperbarui', paras: P(`Halo <b>${esc(d.name)}</b>, jadwal lelang terbaru:`, `<b>${esc(fdt(Date.parse(d.sched + ':00+07:00')))}</b>`), cta: ['Lihat Lelang'] };
    case 'reminder': return { title: 'Lelang dimulai 10 menit lagi!', paras: P(`Halo <b>${esc(d.name)}</b>, lelang <b>${esc(p)}</b> akan dimulai pada <b>${esc(fdt(Date.parse(d.sched + ':00+07:00')))}</b>.`, `Harga awal <b>${fmt(d.start)}</b>. Siapkan diri dan buka halaman lelang sebelum waktu mulai.`), cta: ['Buka Halaman Lelang'] };
    case 'started': return { title: 'Lelang SIRAMIN sudah LIVE!', paras: P(`Halo <b>${esc(d.name)}</b>, lelang yang Anda ikuti sekarang <b>LIVE</b>!`, `Harga awal <b>${fmt(d.start)}</b>. Lelang berakhir pada <b>${esc(fdt(d.endsAt))}</b>.`, 'Segera masukkan penawaran Anda.'), cta: ['🔥 IKUTI LELANG SEKARANG'] };
    case 'win': return { title: 'Selamat! Anda memenangkan Lelang SIRAMIN 🏆', paras: P(`Halo <b>${esc(d.name)}</b>, Anda adalah pemenang lelang <b>${esc(p)}</b> dengan penawaran <b>${fmt(d.amt)}</b>.`, 'Silakan lanjut ke pembayaran: isi data pengiriman lalu bayar lewat QRIS. Setelah itu tekan <b>Saya Sudah Bayar</b>.'), cta: ['Lanjut ke Pembayaran'] };
    case 'claim': return { title: 'Pembayaran Anda sedang diverifikasi', paras: P(`Halo <b>${esc(d.name)}</b>, terima kasih! Kami menerima konfirmasi pembayaran Anda.`, `Nomor pesanan: <b>${esc(d.orderId)}</b><br>Jumlah: <b>${fmt(d.amt)}</b>`, 'Admin akan memeriksa pembayaran dan mengirim email konfirmasi setelah diterima.'), cta: ['Lihat Status'] };
    case 'paid': return { title: 'Pembayaran SIRAMIN diterima ✅', paras: P(`Halo <b>${esc(d.name)}</b>, pembayaran Anda sudah <b>kami terima</b>.`, `Nomor pesanan: <b>${esc(d.orderId)}</b><br>Jumlah dibayar: <b>${fmt(d.amt)}</b><br>Status pembayaran: <b>Lunas</b>`, 'Selanjutnya: pesanan Anda kami siapkan dan kirim ke alamat yang Anda isi. Kami akan menghubungi lewat WhatsApp/email bila perlu.'), cta: ['Lihat Pesanan'] };
    case 'claimOwner': return { title: 'Pembayaran menunggu konfirmasi 💰', paras: P(`Pelanggan <b>${esc(d.name)}</b> menekan "Saya Sudah Bayar".`, `Pesanan: <b>${esc(d.orderId)}</b><br>Jumlah: <b>${fmt(d.amt)}</b><br>WhatsApp: ${esc(d.phone)}<br>Email: ${esc(d.email)}<br>Alamat: ${esc(d.address)}`, 'Cek mutasi/QRIS Anda, lalu buka Dashboard untuk <b>Konfirmasi</b> atau <b>Tolak</b>.'), cta: ['Buka Dashboard'] };
    case 'paidOwner': return { title: 'Pembayaran dikonfirmasi ✅', paras: P(`Pesanan <b>${esc(d.orderId)}</b> dari <b>${esc(d.name)}</b> sebesar <b>${fmt(d.amt)}</b> sudah ditandai LUNAS.`), cta: ['Buka Dashboard'] };
    case 'test': return { title: 'Email uji SIRAMIN berhasil', paras: P('Jika Anda membaca ini, pengiriman email dari server sudah benar. 🎉'), cta: null };
    default: return { title: 'SIRAMIN', paras: P('Pesan dari SIRAMIN.'), cta: null };
  }
}
function html(kind, d) {
  const r = recipe(kind, d), url = SITE();
  return `<!doctype html><html><body style="margin:0;background:#F3F1E6;font-family:Arial,Helvetica,sans-serif;color:#1F2A1C"><table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr><td align="center" style="padding:24px 12px"><table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:560px;background:#ffffff;border-radius:16px;overflow:hidden"><tr><td style="background:#2F5D2E;padding:22px 24px;color:#ffffff"><div style="font-size:26px;font-weight:700;letter-spacing:1px">SIRAMIN</div><div style="font-size:12px;opacity:.85;margin-top:2px">Sistem Irigasi Ramah Lingkungan dan Inovatif</div></td></tr><tr><td style="padding:24px"><h2 style="margin:0 0 14px;font-size:20px;color:#2F5D2E">${esc(r.title)}</h2>${r.paras.map(x => `<p style="margin:0 0 12px;line-height:1.55;font-size:15px">${x}</p>`).join('')}${r.cta && url ? `<p style="margin:22px 0 4px"><a href="${esc(url)}" style="background:#2F5D2E;color:#ffffff;text-decoration:none;padding:13px 22px;border-radius:12px;font-weight:700;display:inline-block">${esc(r.cta[0])}</a></p>` : ''}</td></tr><tr><td style="background:#F3F1E6;padding:14px 24px;font-size:12px;color:#667060">Email otomatis dari SIRAMIN. Pertanyaan? Hubungi SiraminOfficial@gmail.com.</td></tr></table></td></tr></table></body></html>`;
}
let customMailer = null;
async function smtp(to, subject, body) {
  const u = env('EMAIL_USER'), pw = env('EMAIL_APP_PASSWORD').replace(/\s/g, '');
  if (!u || !pw) throw new Error('EMAIL_USER / EMAIL_APP_PASSWORD belum diisi');
  const t = require('nodemailer').createTransport({ host: 'smtp.gmail.com', port: 465, secure: true, auth: { user: u, pass: pw }, connectionTimeout: 8000, greetingTimeout: 8000, socketTimeout: 12000 });
  await t.sendMail({ from: `"SIRAMIN" <${u}>`, to, subject, html: body });
}
// Sends pending emails. An email is claimed ("sending") inside the lock first, so two requests never send the same email twice.
async function flush() {
  let claim = [];
  await locked(db => {
    const n = nowMs(), o = db.auction.outbox;
    o.forEach(e => { if (e.status === 'sending' && n - e.ts > 120000) e.status = 'pending'; });
    claim = o.filter(e => e.status === 'pending' && e.tries < 5).slice(0, 8);
    claim.forEach(e => { e.status = 'sending'; e.ts = n; e.tries++; });
    if (!claim.length) db.__nochange = true;
  });
  if (!claim.length) return;
  const send = customMailer || smtp;
  const res = await Promise.all(claim.map(async e => { try { await send(e.to, e.subject, html(e.kind, e.data)); return [e.id, 'sent', '']; } catch (x) { return [e.id, 'failed', String((x && x.message) || x).slice(0, 140)]; } }));
  await locked(db => { for (const [id, st, er] of res) { const e = db.auction.outbox.find(z => z.id === id); if (e) { e.status = st; e.err = er; } } });
}

// ---------------- auction state machine (absolute timestamps only) ----------------
function begin(db, by, at) {
  const a = db.auction; a.bids = []; a.out = []; a.status = 'active'; a.startedAt = at; a.startedBy = by; a.endsAt = at + a.durMin * 60000;
  a.followers.filter(f => f.auctionId === aid(a)).forEach(f => {
    if (!f.auctionStartedEmailSent) { f.auctionStartedEmailSent = true; f.auctionStartedEmailSentAt = nowMs(); queue(db, f.email, '🚨 Lelang SIRAMIN sudah LIVE!', 'started', { name: f.name, start: a.start, endsAt: a.endsAt }); }
    notify(a, f.name, 'Lelang yang Anda ikuti sudah dimulai! Ayo menawar sekarang.');
  });
}
function announceWinner(db, w) {
  const a = db.auction, x = userByName(db, w.name);
  notify(a, w.name, 'Selamat! Anda memenangkan lelang. Silakan lanjut ke pembayaran.');
  if (x) queue(db, x.email, '🏆 Selamat! Anda memenangkan Lelang SIRAMIN', 'win', { name: w.name, amt: w.amt });
}
function end(db) {
  const a = db.auction, w = win(a);
  a.history.push({ n: a.history.length + 1, winner: w ? w.name : null, amt: w ? w.amt : null, bids: a.bids.length, result: w ? 'Selesai' : 'Tanpa penawaran', t: nowMs() });
  a.status = 'ended'; if (w) announceWinner(db, w);
}
function due(db) {
  const a = db.auction, n = nowMs();
  if (a.status === 'active' && n >= a.endsAt) return true;
  if (a.status === 'idle' && a.sched && a.stock > 0) { const m = schedMs(a); if (n >= m) return true; if (n >= m - 600000 && a.followers.some(f => f.auctionId === aid(a) && !f.reminderEmailSent)) return true; }
  return false;
}
function advance(db) {
  const a = db.auction, n = nowMs(); let ch = false;
  if (a.status === 'idle' && a.sched && a.stock > 0) {
    const m = schedMs(a);
    if (n >= m - 600000 && n < m) a.followers.filter(f => f.auctionId === aid(a) && !f.reminderEmailSent).forEach(f => {      // reminder, exactly once per follower
      f.reminderEmailSent = true; ch = true; queue(db, f.email, '⏰ Lelang SIRAMIN dimulai 10 menit lagi', 'reminder', { name: f.name, sched: a.sched, start: a.start });
      notify(a, f.name, 'Lelang dimulai 10 menit lagi!'); });
    if (n >= m) { begin(db, 'Jadwal otomatis', m); ch = true; }
  }
  if (a.status === 'active' && n >= a.endsAt) { end(db); ch = true; }
  return ch;
}
const durText = a => (a.durMin % 1440 === 0 ? a.durMin / 1440 + ' hari' : a.durMin % 60 === 0 ? a.durMin / 60 + ' jam' : a.durMin + ' menit');

// ---------------- what each viewer may see ----------------
function pubAuction(a) {
  return { status: a.status, start: a.start, inc: a.inc, stock: a.stock, sched: a.sched, durMin: a.durMin, startedAt: a.startedAt, startedBy: a.startedBy, endsAt: a.endsAt,
    bids: a.bids.map(b => ({ name: b.name, amt: b.amt, t: b.t })), out: a.out, round: a.round, history: a.history, rev: a.rev,
    followers: a.followers.map(f => ({ auctionId: f.auctionId, name: f.name })) };
}
function resp(db, u, extra) {
  const a = db.auction, r = { ok: true, now: nowMs(), rev: a.rev, auction: pubAuction(a), ...(extra || {}) };
  if (u) {
    r.priv = true; r.me = { name: u.name, role: u.role };
    r.notifs = a.notifs.filter(n => n.to === u.name && !n.seen);
    const mo = [...a.orders].reverse().find(o => o.account === u.name && o.round === a.round && o.status !== 'rejected');
    r.myOrder = mo ? { ...mo } : null;
    if (u.role === 'owner') r.owner = { followers: a.followers, orders: a.orders.slice(-40), outbox: a.outbox.slice(-40).map(e => ({ id: e.id, to: e.to, subject: e.subject, status: e.status, err: e.err, t: e.t })) };
  }
  return r;
}
const err = (error, code) => ({ error, code: code || 400 });

// ---------------- auth ----------------
const hashPw = (p, s) => crypto.pbkdf2Sync(p, s, 120000, 32, 'sha256').toString('hex');
const same = (x, y) => { const a = Buffer.from(String(x)), b = Buffer.from(String(y)); return a.length === b.length && crypto.timingSafeEqual(a, b); };
const tokenOf = h => { const a = (h && h.authorization) || ''; return a.startsWith('Bearer ') ? a.slice(7) : ''; };
async function userOf(h) { const t = tokenOf(h); if (!t) return null; const raw = await rc('GET', 'tok:' + t); return raw ? JSON.parse(raw) : null; }
async function limited(k, max, ttl) { const n = await rc('INCR', k); if (n === 1) await rc('EXPIRE', k, ttl); return n > max; }

// ---------------- all write actions (run inside the lock) ----------------
function act(db, p, b, u) {
  const a = db.auction, n = nowMs(); advance(db);

  if (p === 'register') {
    const name = String(b.username || '').trim(), em = String(b.email || '').trim(), pw = String(b.password || ''), k = name.toLowerCase();
    if (!/^[\w.-]{3,20}$/.test(name)) return err('Username: 3–20 karakter (huruf, angka, . _ -)');
    if (!/^\S+@\S+\.\S+$/.test(em)) return err('Masukkan alamat email yang valid.');
    if (pw.length < 6) return err('Kata sandi minimal 6 karakter.');
    if (k === OWNER_USER() || k === 'siraminofficial') return err('Username atau email ini dicadangkan.');
    if (db.users[k]) return err('Username sudah dipakai.');
    if (Object.values(db.users).some(x => x.email.toLowerCase() === em.toLowerCase())) return err('Email sudah terdaftar.');
    const salt = crypto.randomBytes(8).toString('hex'); db.users[k] = { name, email: em, salt, h: hashPw(pw, salt) };
    return { ...resp(db, { name, role: 'user' }), name, role: 'user', _tok: { name, role: 'user' } };
  }
  if (p === 'login') {
    const name = String(b.username || '').trim(), pw = String(b.password || ''), k = name.toLowerCase(), op = env('OWNER_PASSWORD');
    if (op && k === OWNER_USER() && same(pw, op)) { db.__nochange = true; return { ...resp(db, { name: 'SIRAMIN Official', role: 'owner' }), name: 'SIRAMIN Official', role: 'owner', _tok: { name: 'SIRAMIN Official', role: 'owner' } }; }
    const x = db.users[k]; if (!x || !same(hashPw(pw, x.salt), x.h)) return err('Username atau kata sandi salah.', 401);
    db.__nochange = true; return { ...resp(db, { name: x.name, role: 'user' }), name: x.name, role: 'user', _tok: { name: x.name, role: 'user' } };
  }
  if (!u) return err('Silakan masuk terlebih dahulu.', 401);
  const owner = u.role === 'owner';

  if (p === 'seen') { const x = a.notifs.find(z => z.id === b.id && z.to === u.name); if (x) x.seen = true; else db.__nochange = true; return resp(db, u); }

  if (p.startsWith('owner/')) {
    if (!owner) return err('Akses ditolak.', 403);
    switch (p) {
      case 'owner/start': if (a.status !== 'idle' || a.stock <= 0) return err('Lelang tidak bisa dimulai sekarang.'); begin(db, u.name, n); break;
      case 'owner/cancel': { if (a.status !== 'active') return err('Lelang tidak sedang berlangsung.'); const l = a.bids[a.bids.length - 1];
        a.history.push({ n: a.history.length + 1, winner: null, amt: l ? l.amt : null, bids: a.bids.length, result: 'Dibatalkan', t: n }); a.status = 'cancelled'; a.endsAt = null; break; }
      case 'owner/newround': if (!['ended', 'cancelled'].includes(a.status) || a.stock <= 0) return err('Belum bisa menyiapkan lelang baru.');
        a.bids = []; a.out = []; a.round++; a.status = 'idle'; a.endsAt = null; a.startedAt = null; a.startedBy = null; a.sched = null; break;
      case 'owner/schedule': {
        if (a.status !== 'idle') return err('Jadwal hanya bisa diubah sebelum lelang dimulai.');
        if (b.sched === null) { a.sched = null; break; }
        const v = String(b.sched || ''); if (!/^\d{4}-\d\d-\d\dT\d\d:\d\d$/.test(v) || isNaN(Date.parse(v + ':00+07:00'))) return err('Isi tanggal dan jam mulai');
        if (Date.parse(v + ':00+07:00') <= n) return err('Jadwal harus di masa depan');
        if (a.sched !== v) { a.sched = v; a.followers.filter(f => f.auctionId === aid(a)).forEach(f => { f.reminderEmailSent = false;
          queue(db, f.email, '📅 Jadwal Lelang SIRAMIN diperbarui', 'schedule', { name: f.name, sched: v }); notify(a, f.name, 'Jadwal lelang: mulai ' + fdt(Date.parse(v + ':00+07:00'))); }); }
        advance(db); break; }
      case 'owner/settings': { if (a.status === 'active') return err('Terkunci saat lelang berlangsung');
        const s = Math.floor(+b.start), i = Math.floor(+b.inc), d = Math.floor(+b.durMin);
        if (s > 0) a.start = s; if (i > 0) a.inc = i; if (d > 0 && d <= 43200) a.durMin = d; break; }
      case 'owner/reset': db.auction = fresh(); break;
      case 'owner/payment': {
        const o = a.orders.find(z => z.id === b.id); if (!o || o.status !== 'awaiting') return err('Pesanan tidak ditemukan atau sudah diproses.');
        if (b.action === 'confirm') { o.status = 'paid'; o.paidAt = n; a.stock = 0; const hl = a.history[a.history.length - 1]; if (hl) hl.result = 'Terjual';
          queue(db, o.email, '✅ Pembayaran SIRAMIN diterima — ' + o.id, 'paid', { name: o.name, orderId: o.id, amt: o.amount }); queue(db, OWNER_EMAIL(), '✅ Pembayaran dikonfirmasi — ' + o.id, 'paidOwner', { name: o.name, orderId: o.id, amt: o.amount });
          notify(a, o.account, 'Pembayaran Anda sudah dikonfirmasi. Terima kasih!'); }
        else if (b.action === 'reject') { o.status = 'rejected'; notify(a, o.account, 'Pembayaran belum dapat dikonfirmasi. Silakan cek dan coba lagi atau hubungi admin.'); }
        else return err('Aksi tidak dikenal.');
        break; }
      case 'owner/pass': { const w = win(a);
        if (a.status !== 'ended' || a.stock <= 0 || !w || hasOrder(a)) return err('Barang tidak bisa dioper sekarang.');
        a.out.push(w.name); const nx = win(a); if (nx) announceWinner(db, nx);
        const hl = a.history[a.history.length - 1]; if (hl) { hl.winner = nx ? nx.name : null; hl.amt = nx ? nx.amt : null; hl.result = nx ? 'Dioper' : 'Tanpa pemenang'; } break; }
      case 'owner/resend': a.outbox.forEach(e => { if (e.status === 'failed') { e.status = 'pending'; e.tries = 0; e.err = ''; } }); break;
      case 'owner/testmail': queue(db, OWNER_EMAIL(), 'Email uji SIRAMIN', 'test', {}); break;
      default: return err('Tidak ditemukan.', 404);
    }
    return resp(db, u);
  }

  if (owner) return err('Owner tidak dapat menawar.', 403);
  if (p === 'bid') {
    if (a.status === 'idle') return err('Lelang belum dimulai.');
    if (a.stock <= 0 || a.status !== 'active' || n >= a.endsAt) return err('Penawaran sudah ditutup.');
    const amt = Math.floor(+b.amt), last = a.bids[a.bids.length - 1];
    if (!amt) return err('Masukkan jumlah penawaran yang valid.');
    if (last && last.name === u.name) return err('Anda sudah menjadi penawar tertinggi.');
    if (last && amt <= last.amt) return err('Penawaran Anda harus lebih tinggi dari penawaran tertinggi saat ini.');
    const min = last ? last.amt + a.inc : a.start;
    if (amt < min) return err((last ? 'Kenaikan minimum adalah ' : 'Harga awal adalah ') + fmt(last ? a.inc : a.start) + '. Tawar minimal ' + fmt(min) + '.');
    a.bids.push({ name: u.name, amt, t: n }); return resp(db, u);
  }
  if (p === 'follow') {
    if (a.status !== 'idle') return err('Lelang sudah dimulai.');
    const x = userByName(db, u.name); if (!x) return err('Akun tidak ditemukan.');
    if (a.followers.some(f => f.auctionId === aid(a) && f.email.toLowerCase() === x.email.toLowerCase())) return err('Anda sudah mengikuti lelang ini');
    const m = schedMs(a), f = { followerId: 'FOL-' + crypto.randomBytes(4).toString('hex'), auctionId: aid(a), email: x.email, name: u.name, followedAt: n,
      confirmationEmailSent: true, reminderEmailSent: !!(m && n >= m - 600000), auctionStartedEmailSent: false, auctionStartedEmailSentAt: null };
    a.followers.push(f); queue(db, x.email, '🔔 Anda mengikuti Lelang SIRAMIN', 'confirm', { name: u.name, aid: aid(a), start: a.start, dur: durText(a), sched: a.sched });
    return resp(db, u);
  }
  if (p === 'win/pass' || p === 'pay/claim') {
    const w = win(a);
    if (a.status !== 'ended' || a.stock <= 0 || !w || w.name !== u.name) return err('Hanya pemenang yang dapat melakukan ini.', 403);
    if (hasOrder(a)) return err('Pembayaran sudah diajukan.');
    if (p === 'pay/claim') {
      const o = b.order || {}, cl = (v, m) => String(v == null ? '' : v).trim().slice(0, m);
      const rec = { name: cl(o.name, 80), phone: cl(o.phone, 30), email: cl(o.email, 80), address: cl(o.address, 250), city: cl(o.city, 60), zip: cl(o.zip, 10), notes: cl(o.notes, 250) };
      if (!rec.name || rec.phone.replace(/\D/g, '').length < 9 || !/^\S+@\S+\.\S+$/.test(rec.email) || !rec.address || !rec.city || !/^\d{5}$/.test(rec.zip)) return err('Data pesanan belum lengkap.');
      const id = 'ORD-' + crypto.randomBytes(3).toString('hex').toUpperCase();
      a.orders.push({ id, account: u.name, ...rec, amount: w.amt, status: 'awaiting', round: a.round, t: n });
      queue(db, rec.email, 'Pembayaran Anda sedang diverifikasi — ' + id, 'claim', { name: rec.name, orderId: id, amt: w.amt });
      queue(db, OWNER_EMAIL(), '💰 Pembayaran menunggu konfirmasi — ' + id, 'claimOwner', { name: rec.name, orderId: id, amt: w.amt, phone: rec.phone, email: rec.email, address: rec.address + ', ' + rec.city + ' ' + rec.zip });
      return resp(db, u);
    }
    a.out.push(u.name); const nx = win(a); if (nx) announceWinner(db, nx);
    const hl = a.history[a.history.length - 1]; if (hl) { hl.winner = nx ? nx.name : null; hl.amt = nx ? nx.amt : null; hl.result = nx ? 'Dioper' : 'Tanpa pemenang'; }
    return resp(db, u);
  }
  return err('Tidak ditemukan.', 404);
}

async function mutate(p, b, h, ip) {
  if (p === 'login' && (await limited('rl:login:' + ip, 15, 600))) return err('Terlalu banyak percobaan. Coba lagi beberapa menit lagi.', 429);
  if (p === 'register' && (await limited('rl:reg:' + ip, 15, 600))) return err('Terlalu banyak percobaan. Coba lagi beberapa menit lagi.', 429);
  const u = await userOf(h); let out;
  await locked(db => { out = act(db, p, b || {}, u); if (out && out.error) db.__nochange = true; });
  if (out && out._tok) { const t = crypto.randomBytes(24).toString('hex'); await rc('SET', 'tok:' + t, JSON.stringify(out._tok), 'EX', 2592000); delete out._tok; out.token = t; }
  if (p === 'logout') { const t = tokenOf(h); if (t) await rc('DEL', 'tok:' + t); return { ok: true }; }
  try { await flush(); } catch (e) { console.error('flush failed', e.message); }
  return out;
}
async function maintain() { await locked(db => { if (!advance(db)) db.__nochange = true; }); try { await flush(); } catch (e) { console.error('flush failed', e.message); } }
const pending = db => db.auction.outbox.some(e => e.status === 'pending');

async function route(method, p, q, body, headers, ip) {
  p = String(p || '').replace(/^\/+|\/+$/g, '');
  if (method === 'GET') {
    if (p === 'health') {
      const out = { ok: true, ownerPassword: !!env('OWNER_PASSWORD'), emailUser: !!env('EMAIL_USER'), emailAppPassword: !!env('EMAIL_APP_PASSWORD'), cronSecret: !!env('CRON_SECRET'), siteUrl: SITE() || null, redis: false };
      try { out.redis = (await rc('PING')) === 'PONG'; } catch (e) { out.redisError = String(e.message).slice(0, 120); }
      return [200, out, { 'Cache-Control': 'no-store' }];
    }
    if (p === 'cron') {
      const s = env('CRON_SECRET'), got = q.key || tokenOf(headers);
      if (!s) return [503, { error: 'CRON_SECRET belum diisi' }];
      if (!same(got || '', s)) return [401, { error: 'Kunci salah' }];
      await maintain(); return [200, { ok: true, t: nowMs() }, { 'Cache-Control': 'no-store' }];
    }
    if (p === 'pub' || p === 'state') {
      let db = await readCached();
      if (due(db) || pending(db)) { await maintain(); db = await readCached(true); }
      if (p === 'pub') return [200, resp(db, null), { 'Cache-Control': 'public, s-maxage=1, stale-while-revalidate=2' }];
      const u = tokenOf(headers) ? await userOf(headers) : null;
      return [200, tokenOf(headers) && !u ? { ...resp(db, null), priv: true, me: null } : resp(db, u), { 'Cache-Control': 'no-store' }];
    }
    return [404, { error: 'Tidak ditemukan.' }];
  }
  if (method === 'POST') {
    const out = await mutate(p, body, headers, ip);
    if (out && out.error) return [out.code || 400, { error: out.error }, { 'Cache-Control': 'no-store' }];
    return [200, out, { 'Cache-Control': 'no-store' }];
  }
  return [405, { error: 'Metode tidak didukung.' }];
}

async function readBody(req) { let raw = ''; for await (const c of req) { raw += c; if (raw.length > 1e5) break; } try { return raw ? JSON.parse(raw) : {}; } catch (e) { return {}; } }
async function handler(req, res) {
  try {
    const u = new URL(req.url, 'http://x');
    const p = u.searchParams.get('p') || u.pathname.replace(/^\/api\//, '');
    const q = Object.fromEntries(u.searchParams);
    let body = {}; if (req.method === 'POST') body = req.body && typeof req.body === 'object' ? req.body : typeof req.body === 'string' ? JSON.parse(req.body || '{}') : await readBody(req);
    const ip = String(req.headers['x-forwarded-for'] || '0').split(',')[0].trim();
    const [st, obj, hd] = await route(req.method, p, q, body, req.headers, ip);
    res.statusCode = st; res.setHeader('Content-Type', 'application/json'); res.setHeader('Cache-Control', (hd && hd['Cache-Control']) || 'no-store'); res.end(JSON.stringify(obj));
  } catch (e) {
    console.error(e); res.statusCode = 500; res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify({ error: 'Kesalahan server: ' + String(e.message).slice(0, 160) }));
  }
}
module.exports = handler;
module.exports.route = route;
module.exports.setMailer = f => { customMailer = f; };
module.exports.resetCache = () => { cache = { t: 0, db: null }; };
