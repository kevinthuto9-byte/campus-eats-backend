require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const nodemailer = require('nodemailer');
const { MongoClient } = require('mongodb');
const { v2: cloudinary } = require('cloudinary');
const { OAuth2Client } = require('google-auth-library');

const app = express();
app.set('trust proxy', 1);
const server = http.createServer(app);

// ==================== CONFIGURATION ====================
const csv = (v) => String(v || '').split(',').map((s) => s.trim().toLowerCase()).filter(Boolean);

const PORT = Number(process.env.PORT) || 4000;
const IS_PROD = process.env.NODE_ENV === 'production';
const SESSION_DAYS = Number(process.env.SESSION_DAYS) || 30;
const ORDER_EXPIRY_MINUTES = Number(process.env.ORDER_EXPIRY_MINUTES) || 60;
const SCHEDULED_EXPIRY_HOURS = 12;
const MAX_ACTIVE_DELIVERIES = Number(process.env.MAX_ACTIVE_DELIVERIES) || 3;
const ENFORCE_EMAIL_VERIFICATION = process.env.ENFORCE_EMAIL_VERIFICATION === 'true';
const REQUIRE_TERMS_ACCEPTANCE = process.env.REQUIRE_TERMS_ACCEPTANCE === 'true';
const ALLOWED_EMAIL_DOMAINS = csv(process.env.ALLOWED_EMAIL_DOMAINS);
const ADMIN_EMAILS = csv(process.env.ADMIN_EMAILS);

const HOST_IP = process.env.HOST_IP || '10.205.101.41';
const LOCAL_UTC_OFFSET_HOURS = process.env.LOCAL_UTC_OFFSET_HOURS ? Number(process.env.LOCAL_UTC_OFFSET_HOURS) : 2;

const MONGODB_URI = process.env.MONGODB_URI;
const MONGODB_DB = process.env.MONGODB_DB || 'campus_eats';
if (!MONGODB_URI) {
  console.error('❌ MONGODB_URI is not set. Add it to your .env file.');
  process.exit(1);
}

const GOOGLE_WEB_CLIENT_ID = String(process.env.GOOGLE_WEB_CLIENT_ID || '').trim();
const googleClient = GOOGLE_WEB_CLIENT_ID ? new OAuth2Client(GOOGLE_WEB_CLIENT_ID) : null;

const CLOUD_ENABLED = !!process.env.CLOUDINARY_URL;
if (CLOUD_ENABLED) {
  cloudinary.config({ secure: true });
} else if (IS_PROD) {
  console.warn('⚠️  CLOUDINARY_URL is not set: uploads saved on local disk.');
}

const PICKUP_PRICES = { 'Main Gate': 10, 'School': 8, 'Sir Ketumile': 13, 'Other': 15 };
const FREE_PLATES = 2;
const EXTRA_PLATE_FEE = 5;
const MAX_FEE = 500;

const ACTIVE_STATUSES = ['OPEN', 'CLAIMED', 'PICKED_UP', 'ON_THE_WAY'];
const CHAT_STATUSES = ['CLAIMED', 'PICKED_UP', 'ON_THE_WAY'];
const NEXT_STATUS = { CLAIMED: 'PICKED_UP', PICKED_UP: 'ON_THE_WAY', ON_THE_WAY: 'DELIVERED' };
const STATUS_TIME_FIELD = { PICKED_UP: 'pickedUpAt', ON_THE_WAY: 'onTheWayAt', DELIVERED: 'deliveredAt' };
const REPORT_REASONS = ['HARASSMENT', 'FRAUD', 'UNSAFE', 'NO_SHOW', 'OTHER'];

const baseUrl = (req) => (process.env.PUBLIC_URL
  ? process.env.PUBLIC_URL.replace(/\/$/, '')
  : `${req.protocol}://${req.get('host')}`);

// ==================== SMALL HELPERS ====================
const cleanStr = (v, max) => String(v ?? '').trim().slice(0, max);
const nowIso = () => new Date().toISOString();
const newId = (prefix) => `${prefix}_${crypto.randomBytes(8).toString('hex')}`;
const sha256 = (v) => crypto.createHash('sha256').update(String(v)).digest('hex');
const safeEqual = (a, b) => {
  const x = Buffer.from(String(a));
  const y = Buffer.from(String(b));
  return x.length === y.length && crypto.timingSafeEqual(x, y);
};
const isDuplicateKey = (err) => !!err && err.code === 11000;
const round2 = (n) => Math.round(n * 100) / 100;

const periodStarts = () => {
  const offsetMs = LOCAL_UTC_OFFSET_HOURS * 3600000;
  const local = new Date(Date.now() + offsetMs);
  const dayStart = Date.UTC(local.getUTCFullYear(), local.getUTCMonth(), local.getUTCDate()) - offsetMs;
  const daysSinceMonday = (local.getUTCDay() + 6) % 7;
  const weekStart = dayStart - daysSinceMonday * 86400000;
  const monthStart = Date.UTC(local.getUTCFullYear(), local.getUTCMonth(), 1) - offsetMs;
  return { dayStart, weekStart, monthStart };
};

// ==================== FILE STORAGE ====================
const uploadsDir = path.join(__dirname, 'uploads');
const voicesDir = path.join(uploadsDir, 'voices');
const profilesDir = path.join(uploadsDir, 'profiles');
[uploadsDir, voicesDir, profilesDir].forEach((d) => { if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true }); });

const uploadToCloud = (buffer, options) => new Promise((resolve, reject) => {
  cloudinary.uploader.upload_stream(options, (err, result) => (err ? reject(err) : resolve(result))).end(buffer);
});

const deleteFromCloud = async (publicId, resourceType = 'image') => {
  if (!CLOUD_ENABLED || !publicId) return;
  try {
    await cloudinary.uploader.destroy(publicId, { resource_type: resourceType, invalidate: true });
  } catch (err) {
    console.error('Cloud delete failed:', err.message);
  }
};

const removeProfilePicture = async (user) => {
  if (user.profilePictureId) {
    await deleteFromCloud(user.profilePictureId, 'image');
    return;
  }
  const oldName = user.profilePicture ? path.basename(String(user.profilePicture)) : null;
  if (oldName && oldName.startsWith(`profile_${user.id}_`)) fs.unlink(path.join(profilesDir, oldName), () => {});
};

// ==================== DATABASE (MongoDB) ====================
const mongo = new MongoClient(MONGODB_URI);
const C = {};

const connectDb = async () => {
  await mongo.connect();
  const db = mongo.db(MONGODB_DB);
  C.users = db.collection('users');
  C.sessions = db.collection('sessions');
  C.tasks = db.collection('tasks');
  C.messages = db.collection('messages');
  C.locations = db.collection('locations');
  C.pushTokens = db.collection('pushTokens');
  C.reviews = db.collection('reviews');
  C.reports = db.collection('reports');
  C.blocks = db.collection('blocks');
  C.notifications = db.collection('notifications');

  await Promise.all([
    C.users.createIndex({ email: 1 }, { unique: true }),
    C.users.createIndex({ googleId: 1 }, { sparse: true }),
    C.sessions.createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 }),
    C.sessions.createIndex({ userId: 1 }),
    C.tasks.createIndex({ status: 1, createdAt: -1 }),
    C.tasks.createIndex({ requesterId: 1, createdAt: -1 }),
    C.tasks.createIndex({ runnerId: 1, status: 1 }),
    C.messages.createIndex({ taskId: 1, createdAt: 1 }),
    C.locations.createIndex({ userId: 1 }),
    C.pushTokens.createIndex({ pushToken: 1 }, { unique: true }),
    C.pushTokens.createIndex({ userId: 1 }),
    C.reviews.createIndex({ taskId: 1, authorId: 1 }, { unique: true }),
    C.reviews.createIndex({ targetId: 1, createdAt: -1 }),
    C.reports.createIndex({ createdAt: -1 }),
    C.blocks.createIndex({ blockerId: 1, blockedId: 1 }, { unique: true }),
    C.blocks.createIndex({ blockedId: 1 }),
    C.notifications.createIndex({ userId: 1, createdAt: -1 }),
    C.notifications.createIndex({ userId: 1, read: 1 }),
  ]);
  console.log(`🍃 Connected to MongoDB (database: ${MONGODB_DB})`);
};

const shutdown = async () => {
  try { await mongo.close(); } catch {}
  process.exit(0);
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// ==================== PASSWORD HASHING ====================
const scryptAsync = (password, salt) => new Promise((resolve, reject) => {
  crypto.scrypt(String(password), salt, 64, (err, key) => (err ? reject(err) : resolve(key)));
});

const hashPassword = async (password) => {
  const salt = crypto.randomBytes(16);
  const key = await scryptAsync(password, salt);
  return `${salt.toString('hex')}:${key.toString('hex')}`;
};

const verifyPassword = async (password, stored) => {
  if (!stored) return false;
  const parts = stored.split(':');
  if (parts.length !== 2) return false;
  const [salt, hash] = parts;
  const candidate = await scryptAsync(password, Buffer.from(salt, 'hex'));
  const original = Buffer.from(hash, 'hex');
  return candidate.length === original.length && crypto.timingSafeEqual(candidate, original);
};

let DUMMY_HASH = null;
hashPassword('dummy-password-for-timing').then((h) => { DUMMY_HASH = h; });

// ==================== USERS / SESSIONS ====================
const normalizeEmail = (email) => String(email || '').trim().toLowerCase();
const isValidEmail = (email) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) && email.length <= 120;
const emailAllowed = (email) =>
  ALLOWED_EMAIL_DOMAINS.length === 0 ||
  ALLOWED_EMAIL_DOMAINS.some((d) => email.endsWith(`@${d}`) || email.endsWith(`.${d}`));

const findUserById = async (id) => (id ? C.users.findOne({ _id: String(id) }) : null);
const findUserByEmail = (email) => C.users.findOne({ email });
const usersByIds = async (ids) => {
  const list = ids.length ? await C.users.find({ _id: { $in: ids.map(String) } }).toArray() : [];
  return new Map(list.map((u) => [u.id, u]));
};

const publicUser = (u) => ({
  id: u.id,
  name: u.name,
  email: u.email,
  role: u.role,
  school: u.school,
  profilePicture: u.profilePicture || null,
  emailVerified: !!u.emailVerified,
  hasPassword: !!u.passwordHash,
});
const minimalUser = (u) => (u ? { id: u.id, name: u.name, profilePicture: u.profilePicture || null, role: u.role } : null);

const createSession = async (userId) => {
  const token = crypto.randomBytes(32).toString('hex');
  await C.sessions.insertOne({
    _id: sha256(token),
    userId,
    createdAt: nowIso(),
    expiresAt: new Date(Date.now() + SESSION_DAYS * 86400000),
  });
  return token;
};

const getSession = async (token) => {
  if (!token) return null;
  const key = sha256(token);
  const s = await C.sessions.findOne({ _id: key });
  if (!s) return null;
  if (new Date(s.expiresAt).getTime() < Date.now()) {
    await C.sessions.deleteOne({ _id: key });
    return null;
  }
  return { ...s, key };
};

const deleteUserSessions = (userId, exceptKey) =>
  C.sessions.deleteMany(exceptKey ? { userId, _id: { $ne: exceptKey } } : { userId });

const verifyGoogleToken = async (idToken) => {
  if (!googleClient) throw new Error('Google sign-in is not configured');
  const ticket = await googleClient.verifyIdToken({ idToken: String(idToken || ''), audience: GOOGLE_WEB_CLIENT_ID });
  return ticket.getPayload();
};

const isTaskParticipant = (task, userId) =>
  !!task && (String(task.requesterId) === String(userId) || String(task.runnerId) === String(userId));

const isBlocked = async (a, b) => {
  if (!a || !b) return false;
  const found = await C.blocks.findOne({
    $or: [{ blockerId: a, blockedId: b }, { blockerId: b, blockedId: a }],
  });
  return !!found;
};

const blockedIdsFor = async (userId) => {
  const rows = await C.blocks.find({ $or: [{ blockerId: userId }, { blockedId: userId }] }).toArray();
  return new Set(rows.map((b) => (b.blockerId === userId ? b.blockedId : b.blockerId)));
};

// ==================== EMAIL ====================
let mailer = null;
if (process.env.SMTP_HOST) {
  mailer = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: Number(process.env.SMTP_PORT) || 587,
    secure: process.env.SMTP_SECURE === 'true',
    auth: process.env.SMTP_USER ? { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS } : undefined,
  });
} else if (IS_PROD) {
  console.warn('⚠️  SMTP is not configured.');
}

const sendEmail = async (to, subject, text) => {
  if (!mailer) {
    console.log(`[DEV EMAIL] To: ${to} | ${subject}\n${text}\n`);
    return true;
  }
  try {
    await mailer.sendMail({ from: process.env.MAIL_FROM || 'Campus Eats <no-reply@campuseats.bw>', to, subject, text });
    return true;
  } catch (err) {
    console.error('Email send failed:', err.message);
    return false;
  }
};

const makeCode = () => String(crypto.randomInt(0, 1000000)).padStart(6, '0');
const hashCode = (email, code) => sha256(`${email}:${code}`);

const issueVerification = async (user) => {
  const code = makeCode();
  await C.users.updateOne(
    { _id: user.id },
    { $set: { verifyCodeHash: hashCode(user.email, code), verifyExpiresAt: Date.now() + 30 * 60 * 1000, verifyAttempts: 0 } }
  );
  await sendEmail(
    user.email,
    'Verify your Campus Eats email',
    `Hi ${user.name},\n\nYour Campus Eats verification code is ${code}.\nIt expires in 30 minutes.\n\nIf you did not create this account, you can ignore this email.`
  );
};

// ==================== RATE LIMITING ====================
const hits = new Map();
const rateLimit = (name, windowMs, max, keyFn = (req) => req.ip) => (req, res, next) => {
  const key = `${name}:${keyFn(req)}`;
  const now = Date.now();
  let entry = hits.get(key);
  if (!entry || entry.resetAt <= now) {
    entry = { count: 0, resetAt: now + windowMs };
    hits.set(key, entry);
  }
  entry.count += 1;
  if (entry.count > max) {
    res.set('Retry-After', String(Math.ceil((entry.resetAt - now) / 1000)));
    return res.status(429).json({ success: false, error: 'Too many attempts. Please try again later.' });
  }
  next();
};
const byIpAndEmail = (req) => `${req.ip}|${normalizeEmail(req.body?.email)}`;
const byEmail = (req) => normalizeEmail(req.body?.email);
const byUser = (req) => req.user.id;

setInterval(() => {
  const now = Date.now();
  hits.forEach((v, k) => { if (v.resetAt <= now) hits.delete(k); });
}, 10 * 60 * 1000).unref();

// ==================== MIDDLEWARE ====================
app.use(cors({ origin: '*' }));

const smallJson = express.json({ limit: '100kb' });
const bigJson = express.json({ limit: '8mb' });
app.use((req, res, next) => (req.path === '/api/auth/update-profile-picture' ? bigJson : smallJson)(req, res, next));

app.use('/uploads', express.static(uploadsDir, {
  index: false,
  dotfiles: 'ignore',
  setHeaders: (res) => res.setHeader('X-Content-Type-Options', 'nosniff'),
}));

app.get('/', (req, res) => res.json({ status: 'Server is running!' }));
app.get('/health', (req, res) => res.json({ ok: true, time: nowIso(), host: HOST_IP }));

const authenticateToken = async (req, res, next) => {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];
  if (!token) return res.status(401).json({ success: false, error: 'No token provided' });
  const session = await getSession(token);
  const user = session && (await findUserById(session.userId));
  if (!user) return res.status(401).json({ success: false, error: 'Session expired. Please log in again.' });
  if (user.suspended) return res.status(403).json({ success: false, error: 'Your account has been suspended. Contact support.' });
  req.user = user;
  req.sessionKey = session.key;
  next();
};

const requireVerified = (req, res, next) => {
  if (ENFORCE_EMAIL_VERIFICATION && !req.user.emailVerified) {
    return res.status(403).json({ success: false, code: 'EMAIL_NOT_VERIFIED', error: 'Please verify your email first.' });
  }
  next();
};

const requireAdmin = (req, res, next) => {
  if (!ADMIN_EMAILS.includes(normalizeEmail(req.user.email))) {
    return res.status(403).json({ success: false, error: 'Not allowed' });
  }
  next();
};

// ==================== SOCKET.IO ====================
const io = new Server(server, { cors: { origin: '*', methods: ['GET', 'POST'] } });

io.use(async (socket, next) => {
  try {
    const session = await getSession(socket.handshake.auth?.token);
    const user = session && (await findUserById(session.userId));
    if (!user || user.suspended) return next(new Error('AUTH_FAILED'));
    socket.data.userId = user.id;
    next();
  } catch (err) {
    console.error('Socket auth error:', err.message);
    next(new Error('AUTH_FAILED'));
  }
});

io.on('connection', (socket) => {
  socket.join(`user_${socket.data.userId}`);

  socket.on('join_task_room', async ({ taskId } = {}) => {
    try {
      const task = await C.tasks.findOne({ _id: String(taskId) });
      if (task && isTaskParticipant(task, socket.data.userId)) socket.join(`task_${taskId}`);
    } catch (err) {
      console.error('join_task_room error:', err.message);
    }
  });
  socket.on('leave_task_room', ({ taskId } = {}) => {
    socket.leave(`task_${taskId}`);
  });
});

const emitToUsers = (userIds, event, payload) => {
  const ids = [...new Set((userIds || []).filter(Boolean).map(String))];
  if (ids.length === 0) return;
  let target = io;
  ids.forEach((id) => { target = target.to(`user_${id}`); });
  target.emit(event, payload);
};

const runnerIdsFor = async (requesterId) => {
  const [runners, blocked] = await Promise.all([
    C.users.find({ role: 'RUNNER', suspended: { $ne: true }, id: { $ne: requesterId } }, { projection: { id: 1 } }).toArray(),
    blockedIdsFor(requesterId),
  ]);
  return runners.map((u) => u.id).filter((id) => !blocked.has(id));
};

// ==================== NOTIFICATIONS ====================
app.get('/api/notifications', authenticateToken, async (req, res) => {
  const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 30, 1), 100);
  const skip = Math.max(parseInt(req.query.skip, 10) || 0, 0);

  const [rows, total, unread] = await Promise.all([
    C.notifications
      .find({ userId: req.user.id })
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(limit)
      .toArray(),
    C.notifications.countDocuments({ userId: req.user.id }),
    C.notifications.countDocuments({ userId: req.user.id, read: false }),
  ]);

  const data = rows.map((n) => {
    const { _id, userId, ...rest } = n;
    return { id: _id, ...rest };
  });

  res.json({
    success: true,
    data: { notifications: data, total, unread, hasMore: skip + data.length < total },
  });
});

app.patch('/api/notifications/:id/read', authenticateToken, async (req, res) => {
  const result = await C.notifications.updateOne(
    { _id: String(req.params.id), userId: req.user.id },
    { $set: { read: true, readAt: nowIso() } }
  );
  if (result.matchedCount === 0) return res.status(404).json({ success: false, error: 'Notification not found' });
  res.json({ success: true });
});

app.patch('/api/notifications/read-all', authenticateToken, async (req, res) => {
  await C.notifications.updateMany(
    { userId: req.user.id, read: false },
    { $set: { read: true, readAt: nowIso() } }
  );
  res.json({ success: true });
});

app.delete('/api/notifications/:id', authenticateToken, async (req, res) => {
  await C.notifications.deleteOne({ _id: String(req.params.id), userId: req.user.id });
  res.json({ success: true });
});

app.delete('/api/notifications', authenticateToken, async (req, res) => {
  await C.notifications.deleteMany({ userId: req.user.id });
  res.json({ success: true });
});

const createNotification = async ({ userId, type, title, body, taskId = null, imageUrl = null }) => {
  if (!userId) return null;
  const id = newId('notif');
  const doc = {
    _id: id,
    id,
    userId,
    type,
    title,
    body,
    taskId,
    imageUrl,
    read: false,
    createdAt: nowIso(),
  };
  try {
    await C.notifications.insertOne(doc);
    emitToUsers([userId], 'notification', { ...doc, userId: undefined });
    return doc;
  } catch (err) {
    console.error('Failed to create notification:', err.message);
    return null;
  }
};

// ==================== PUSH NOTIFICATIONS ====================
const EXPO_PUSH_URL = 'https://exp.host/--/api/v2/push/send';

const isExpoPushToken = (token) => typeof token === 'string' && /^(ExponentPushToken|ExpoPushToken)\[[^\]]+\]$/.test(token);
const removePushTokens = (tokens) => C.pushTokens.deleteMany({ pushToken: { $in: tokens } });

async function sendPushToUsers(userIds, { title, body, data = {} }) {
  try {
    const ids = [...new Set((userIds || []).filter(Boolean).map(String))];
    if (ids.length === 0) return;
    const rows = await C.pushTokens.find({ userId: { $in: ids } }).toArray();
    const tokens = [...new Set(rows.map((p) => p.pushToken))];
    if (tokens.length === 0) return;
    const messages = tokens.map((to) => ({ to, sound: 'default', title, body, data, channelId: 'default', priority: 'high' }));
    const headers = { Accept: 'application/json', 'Content-Type': 'application/json' };
    if (process.env.EXPO_ACCESS_TOKEN) headers.Authorization = `Bearer ${process.env.EXPO_ACCESS_TOKEN}`;
    for (let i = 0; i < messages.length; i += 100) {
      const batch = messages.slice(i, i + 100);
      const res = await fetch(EXPO_PUSH_URL, { method: 'POST', headers, body: JSON.stringify(batch) });
      const json = await res.json();
      const tickets = Array.isArray(json.data) ? json.data : [];
      const deadTokens = [];
      tickets.forEach((ticket, idx) => {
        if (ticket.status === 'error') {
          if (ticket.details && ticket.details.error === 'DeviceNotRegistered') deadTokens.push(batch[idx].to);
          else console.error('Push ticket error:', ticket.message);
        }
      });
      if (deadTokens.length > 0) await removePushTokens(deadTokens);
    }
  } catch (err) {
    console.error('Push send failed:', err.message);
  }
}

// ==================== TASK HELPERS ====================
const parsePlates = (orderDetails, totalItems) => {
  let plates = 0;
  String(orderDetails || '').split('\n').map((l) => l.trim()).filter(Boolean).forEach((line) => {
    if (/^(pickup|scheduled):/i.test(line)) return;
    line.split(/,\s*(?=\d+x\s)/).forEach((part) => {
      const m = part.trim().match(/^(\d+)x\s/);
      if (m) plates += parseInt(m[1], 10);
    });
  });
  if (plates <= 0) plates = Math.max(1, parseInt(totalItems, 10) || 1);
  return Math.min(plates, 50);
};

const minimumFee = (pickupAddress, plates) => {
  const key = Object.prototype.hasOwnProperty.call(PICKUP_PRICES, pickupAddress) ? pickupAddress : 'Other';
  return PICKUP_PRICES[key] + Math.max(0, plates - FREE_PLATES) * EXTRA_PLATE_FEE;
};

const decorateTasks = async (tasks, viewerId) => {
  if (tasks.length === 0) return [];
  const userIds = new Set();
  tasks.forEach((t) => {
    if (t.requesterId) userIds.add(t.requesterId);
    if (t.runnerId) userIds.add(t.runnerId);
  });
  const users = await usersByIds([...userIds]);

  let reviewed = new Set();
  if (viewerId) {
    const rows = await C.reviews
      .find({ authorId: viewerId, taskId: { $in: tasks.map((t) => t._id) } }, { projection: { taskId: 1 } })
      .toArray();
    reviewed = new Set(rows.map((r) => r.taskId));
  }

  return tasks.map((t) => {
    const out = {
      ...t,
      id: t._id,
      requester: minimalUser(users.get(t.requesterId)),
      runner: t.runnerId ? minimalUser(users.get(t.runnerId)) : null,
    };
    if (viewerId) out.reviewedByMe = reviewed.has(t._id);
    return out;
  });
};

const decorateTask = async (task, viewerId) => (await decorateTasks([task], viewerId))[0];

const broadcastStatus = async (task) => {
  const payload = await decorateTask(task);
  emitToUsers([task.requesterId, task.runnerId], 'status_updated', payload);
};

// ==================== AUTH ROUTES ====================
app.post('/api/auth/signup', rateLimit('signup', 60 * 60 * 1000, 10), async (req, res) => {
  const { password, role, school, acceptedTerms } = req.body || {};
  const name = cleanStr(req.body?.name, 60);
  const email = normalizeEmail(req.body?.email);

  if (name.length < 2) return res.status(400).json({ success: false, error: 'Name is required' });
  if (!isValidEmail(email)) return res.status(400).json({ success: false, error: 'Please enter a valid email address' });
  if (!emailAllowed(email)) return res.status(400).json({ success: false, error: 'Please sign up with your university email address' });
  if (!password || String(password).length < 8) return res.status(400).json({ success: false, error: 'Password must be at least 8 characters' });
  if (String(password).length > 100) return res.status(400).json({ success: false, error: 'Password is too long' });
  if (REQUIRE_TERMS_ACCEPTANCE && acceptedTerms !== true) {
    return res.status(400).json({ success: false, error: 'Please accept the Terms of Service and Privacy Policy' });
  }
  if (await findUserByEmail(email)) return res.status(409).json({ success: false, error: 'Email already registered' });

  const id = newId('user');
  const newUser = {
    _id: id,
    id,
    name,
    email,
    passwordHash: await hashPassword(password),
    role: role === 'RUNNER' ? 'RUNNER' : 'CUSTOMER',
    school: cleanStr(school, 80) || 'University of Botswana',
    profilePicture: null,
    emailVerified: false,
    acceptedTermsAt: acceptedTerms === true ? nowIso() : null,
    lateCancels: 0,
    createdAt: nowIso(),
  };
  try {
    await C.users.insertOne(newUser);
  } catch (err) {
    if (isDuplicateKey(err)) return res.status(409).json({ success: false, error: 'Email already registered' });
    throw err;
  }
  await issueVerification(newUser);
  const token = await createSession(newUser.id);
  res.status(201).json({ success: true, data: { user: publicUser(newUser), token } });
});

app.post('/api/auth/login', rateLimit('login', 15 * 60 * 1000, 10, byIpAndEmail), async (req, res) => {
  const email = normalizeEmail(req.body?.email);
  const password = req.body?.password;
  const user = await findUserByEmail(email);
  const ok = await verifyPassword(password, user ? user.passwordHash : DUMMY_HASH);
  if (!user || !ok) return res.status(401).json({ success: false, error: 'Invalid credentials' });
  if (user.suspended) return res.status(403).json({ success: false, error: 'Your account has been suspended. Contact support.' });
  const token = await createSession(user.id);
  res.json({ success: true, data: { user: publicUser(user), token } });
});

app.post('/api/auth/google', rateLimit('google', 15 * 60 * 1000, 30), async (req, res) => {
  if (!googleClient) {
    return res.status(503).json({ success: false, error: 'Google sign-in is not available right now.' });
  }

  let payload;
  try {
    payload = await verifyGoogleToken(req.body?.idToken);
  } catch (err) {
    console.error('Google token check failed:', err.message);
    return res.status(401).json({ success: false, error: 'Google sign-in failed. Please try again.' });
  }

  const email = normalizeEmail(payload?.email);
  if (!payload?.sub || !email || payload.email_verified !== true) {
    return res.status(401).json({ success: false, error: 'Your Google email address is not verified.' });
  }

  let user = (await C.users.findOne({ googleId: payload.sub })) || (await findUserByEmail(email));

  if (user) {
    if (user.suspended) {
      return res.status(403).json({ success: false, error: 'Your account has been suspended. Contact support.' });
    }
    const updates = {};
    if (!user.googleId) updates.googleId = payload.sub;
    if (!user.profilePicture && payload.picture) updates.profilePicture = String(payload.picture).slice(0, 500);
    if (!user.emailVerified && normalizeEmail(user.email) === email) {
      updates.emailVerified = true;
      updates.passwordHash = null;
      await deleteUserSessions(user.id);
    }
    if (Object.keys(updates).length > 0) {
      await C.users.updateOne({ _id: user.id }, { $set: updates });
      user = { ...user, ...updates };
    }
    const token = await createSession(user.id);
    return res.json({ success: true, data: { user: publicUser(user), token, isNewUser: false } });
  }

  const role = req.body?.role;
  if (role !== 'CUSTOMER' && role !== 'RUNNER') {
    return res.json({ success: false, code: 'NEEDS_ROLE' });
  }
  if (!emailAllowed(email)) {
    return res.status(400).json({ success: false, error: 'Please sign up with your university email address' });
  }
  if (REQUIRE_TERMS_ACCEPTANCE && req.body?.acceptedTerms !== true) {
    return res.status(400).json({ success: false, error: 'Please accept the Terms of Service and Privacy Policy' });
  }

  let name = cleanStr(payload.name, 60);
  if (name.length < 2) name = cleanStr(email.split('@')[0], 60);
  if (name.length < 2) name = 'Student';

  const id = newId('user');
  const newUser = {
    _id: id,
    id,
    name,
    email,
    passwordHash: null,
    googleId: payload.sub,
    role,
    school: cleanStr(req.body?.school, 80) || 'University of Botswana',
    profilePicture: payload.picture ? String(payload.picture).slice(0, 500) : null,
    emailVerified: true,
    acceptedTermsAt: req.body?.acceptedTerms === true ? nowIso() : null,
    lateCancels: 0,
    createdAt: nowIso(),
  };
  try {
    await C.users.insertOne(newUser);
  } catch (err) {
    if (isDuplicateKey(err)) return res.status(409).json({ success: false, error: 'Email already registered' });
    throw err;
  }
  const token = await createSession(newUser.id);
  res.status(201).json({ success: true, data: { user: publicUser(newUser), token, isNewUser: true } });
});

app.post(
  '/api/auth/forgot-password',
  rateLimit('forgot-ip', 60 * 60 * 1000, 10),
  rateLimit('forgot-email', 60 * 60 * 1000, 5, byEmail),
  async (req, res) => {
    const email = normalizeEmail(req.body?.email);
    const user = isValidEmail(email) ? await findUserByEmail(email) : null;
    if (user && !user.suspended) {
      const code = makeCode();
      await C.users.updateOne(
        { _id: user.id },
        { $set: { resetCodeHash: hashCode(user.email, code), resetExpiresAt: Date.now() + 15 * 60 * 1000, resetAttempts: 0 } }
      );
      await sendEmail(
        user.email,
        'Reset your Campus Eats password',
        `Hi ${user.name},\n\nYour Campus Eats password reset code is ${code}.\nIt expires in 15 minutes.\n\nIf you did not ask for this, you can ignore this email. Your password has not changed.`
      );
    }
    res.json({ success: true, message: 'If an account exists for that email, a reset code has been sent.' });
  }
);

app.post('/api/auth/reset-password', rateLimit('reset', 60 * 60 * 1000, 10, byIpAndEmail), async (req, res) => {
  const email = normalizeEmail(req.body?.email);
  const code = String(req.body?.code || '').trim();
  const newPassword = req.body?.newPassword;
  const fail = () => res.status(400).json({ success: false, error: 'Invalid or expired code' });

  if (!newPassword || String(newPassword).length < 8) {
    return res.status(400).json({ success: false, error: 'Password must be at least 8 characters' });
  }
  if (String(newPassword).length > 100) return res.status(400).json({ success: false, error: 'Password is too long' });

  const user = await findUserByEmail(email);
  if (!user || !user.resetCodeHash || Date.now() > user.resetExpiresAt) return fail();

  const attempts = (user.resetAttempts || 0) + 1;
  if (attempts > 5) {
    await C.users.updateOne({ _id: user.id }, { $set: { resetCodeHash: null, resetAttempts: attempts } });
    return fail();
  }
  if (!safeEqual(hashCode(user.email, code), user.resetCodeHash)) {
    await C.users.updateOne({ _id: user.id }, { $set: { resetAttempts: attempts } });
    return fail();
  }

  await C.users.updateOne(
    { _id: user.id },
    { $set: { passwordHash: await hashPassword(newPassword), resetCodeHash: null, resetExpiresAt: null, resetAttempts: 0 } }
  );
  await deleteUserSessions(user.id);
  res.json({ success: true, message: 'Password updated. Please log in.' });
});

app.post('/api/auth/change-password', authenticateToken, rateLimit('change-pw', 60 * 60 * 1000, 10, byUser), async (req, res) => {
  const { currentPassword, newPassword } = req.body || {};
  if (!req.user.passwordHash) {
    return res.status(400).json({ success: false, error: 'This account signs in with Google. To add a password, use "Forgot password" on the login screen.' });
  }
  if (!(await verifyPassword(currentPassword, req.user.passwordHash))) {
    return res.status(400).json({ success: false, error: 'Current password is incorrect' });
  }
  if (!newPassword || String(newPassword).length < 8) {
    return res.status(400).json({ success: false, error: 'Password must be at least 8 characters' });
  }
  if (String(newPassword).length > 100) return res.status(400).json({ success: false, error: 'Password is too long' });
  await C.users.updateOne({ _id: req.user.id }, { $set: { passwordHash: await hashPassword(newPassword) } });
  await deleteUserSessions(req.user.id, req.sessionKey);
  res.json({ success: true });
});

app.post('/api/auth/verify-email', authenticateToken, rateLimit('verify', 60 * 60 * 1000, 15, byUser), async (req, res) => {
  const user = req.user;
  if (user.emailVerified) return res.json({ success: true, data: publicUser(user) });
  const code = String(req.body?.code || '').trim();
  const fail = () => res.status(400).json({ success: false, error: 'Invalid or expired code' });

  if (!user.verifyCodeHash || Date.now() > user.verifyExpiresAt) return fail();
  const attempts = (user.verifyAttempts || 0) + 1;
  if (attempts > 5) {
    await C.users.updateOne({ _id: user.id }, { $set: { verifyCodeHash: null, verifyAttempts: attempts } });
    return fail();
  }
  if (!safeEqual(hashCode(user.email, code), user.verifyCodeHash)) {
    await C.users.updateOne({ _id: user.id }, { $set: { verifyAttempts: attempts } });
    return fail();
  }

  await C.users.updateOne({ _id: user.id }, { $set: { emailVerified: true, verifyCodeHash: null, verifyExpiresAt: null } });
  res.json({ success: true, data: publicUser({ ...user, emailVerified: true }) });
});

app.post('/api/auth/resend-verification', authenticateToken, rateLimit('resend-verify', 60 * 60 * 1000, 5, byUser), async (req, res) => {
  if (req.user.emailVerified) return res.json({ success: true });
  await issueVerification(req.user);
  res.json({ success: true });
});

app.post('/api/auth/push-token', authenticateToken, async (req, res) => {
  const { pushToken, platform } = req.body || {};
  if (!isExpoPushToken(pushToken)) return res.status(400).json({ success: false, error: 'Invalid push token' });
  await C.pushTokens.updateOne(
    { pushToken },
    { $set: { userId: req.user.id, platform: cleanStr(platform, 20) || 'unknown' } },
    { upsert: true }
  );
  res.json({ success: true });
});

app.delete('/api/auth/push-token', authenticateToken, async (req, res) => {
  const { pushToken } = req.body || {};
  if (!pushToken) return res.status(400).json({ success: false, error: 'pushToken required' });
  await C.pushTokens.deleteMany({ pushToken, userId: req.user.id });
  res.json({ success: true });
});

app.post('/api/auth/logout', authenticateToken, async (req, res) => {
  await C.sessions.deleteOne({ _id: req.sessionKey });
  res.json({ success: true });
});

app.patch('/api/auth/update-profile', authenticateToken, async (req, res) => {
  const user = req.user;
  const updates = {};
  let emailChanged = false;

  if (req.body?.email) {
    const newEmail = normalizeEmail(req.body.email);
    if (!isValidEmail(newEmail)) return res.status(400).json({ success: false, error: 'Please enter a valid email address' });
    if (!emailAllowed(newEmail)) return res.status(400).json({ success: false, error: 'Please use your university email address' });
    if (newEmail !== normalizeEmail(user.email)) {
      const taken = await C.users.findOne({ email: newEmail, _id: { $ne: user.id } });
      if (taken) return res.status(409).json({ success: false, error: 'Email already registered' });
      updates.email = newEmail;
      updates.emailVerified = false;
      emailChanged = true;
    }
  }
  const name = cleanStr(req.body?.name, 60);
  if (name.length >= 2) updates.name = name;
  const school = cleanStr(req.body?.school, 80);
  if (school) updates.school = school;

  if (Object.keys(updates).length > 0) {
    try {
      await C.users.updateOne({ _id: user.id }, { $set: updates });
    } catch (err) {
      if (isDuplicateKey(err)) return res.status(409).json({ success: false, error: 'Email already registered' });
      throw err;
    }
  }
  const updated = { ...user, ...updates };
  if (emailChanged) await issueVerification(updated);

  res.json({ success: true, data: publicUser(updated) });
});

app.patch('/api/auth/update-profile-picture', authenticateToken, async (req, res) => {
  try {
    const { profilePicture } = req.body || {};
    if (typeof profilePicture !== 'string') return res.status(400).json({ success: false, error: 'No image provided' });
    const match = profilePicture.match(/^data:(image\/(?:jpeg|jpg|png|webp));base64,([A-Za-z0-9+/=]+)$/);
    if (!match) return res.status(400).json({ success: false, error: 'Please use a JPG, PNG or WebP image' });

    const buffer = Buffer.from(match[2], 'base64');
    if (buffer.length > 5 * 1024 * 1024) return res.status(400).json({ success: false, error: 'Image is too large (max 5MB)' });

    let profileUrl;
    let profilePictureId = null;

    if (CLOUD_ENABLED) {
      const result = await uploadToCloud(buffer, {
        folder: 'campus-eats/profiles',
        public_id: `${req.user.id}_${Date.now()}`,
        resource_type: 'image',
        transformation: [{ width: 600, height: 600, crop: 'limit' }, { quality: 'auto' }],
      });
      profileUrl = result.secure_url;
      profilePictureId = result.public_id;
    } else {
      const extension = match[1] === 'image/png' ? 'png' : match[1] === 'image/webp' ? 'webp' : 'jpg';
      const filename = `profile_${req.user.id}_${Date.now()}.${extension}`;
      fs.writeFileSync(path.join(profilesDir, filename), buffer);
      profileUrl = `${baseUrl(req)}/uploads/profiles/${filename}`;
    }

    await removeProfilePicture(req.user);
    await C.users.updateOne({ _id: req.user.id }, { $set: { profilePicture: profileUrl, profilePictureId } });
    res.json({ success: true, data: { profilePicture: profileUrl } });
  } catch (err) {
    console.error('Update profile picture error:', err);
    res.status(500).json({ success: false, error: 'Server error' });
  }
});

app.get('/api/auth/profile/:userId', authenticateToken, async (req, res) => {
  const user = await findUserById(req.params.userId);
  if (!user) return res.status(404).json({ success: false, error: 'User not found' });
  if (user.id === req.user.id) return res.json({ success: true, data: publicUser(user) });
  res.json({ success: true, data: minimalUser(user) });
});

app.post('/api/auth/delete-account', authenticateToken, rateLimit('delete-acct', 60 * 60 * 1000, 5, byUser), async (req, res) => {
  const user = req.user;
  if (user.passwordHash) {
    if (!(await verifyPassword(req.body?.password, user.passwordHash))) {
      return res.status(400).json({ success: false, error: 'Password is incorrect' });
    }
  } else {
    let payload = null;
    try { payload = await verifyGoogleToken(req.body?.idToken); } catch (err) { payload = null; }
    const sameAccount = !!payload && payload.email_verified === true &&
      (payload.sub === user.googleId || normalizeEmail(payload.email) === normalizeEmail(user.email));
    if (!sameAccount) {
      return res.status(400).json({ success: false, error: 'Please confirm with the Google account you signed up with' });
    }
  }
  const hasActive = await C.tasks.findOne({
    status: { $in: ACTIVE_STATUSES },
    $or: [{ requesterId: user.id }, { runnerId: user.id }],
  });
  if (hasActive) {
    return res.status(409).json({ success: false, error: 'Please finish or cancel your active orders before deleting your account.' });
  }

  await removeProfilePicture(user);

  await Promise.all([
    deleteUserSessions(user.id),
    C.pushTokens.deleteMany({ userId: user.id }),
    C.locations.deleteMany({ userId: user.id }),
    C.blocks.deleteMany({ $or: [{ blockerId: user.id }, { blockedId: user.id }] }),
    C.reviews.deleteMany({ $or: [{ authorId: user.id }, { targetId: user.id }] }),
    C.messages.updateMany({ senderId: user.id }, { $set: { senderName: 'Deleted user', senderProfilePicture: null } }),
    C.notifications.deleteMany({ userId: user.id }),
  ]);
  await C.users.deleteOne({ _id: user.id });
  res.json({ success: true });
});

// ==================== TASK ROUTES ====================
app.get('/api/tasks/available', authenticateToken, async (req, res) => {
  const [open, blocked] = await Promise.all([
    C.tasks.find({ status: 'OPEN', requesterId: { $ne: req.user.id } }).sort({ createdAt: -1 }).toArray(),
    blockedIdsFor(req.user.id),
  ]);
  const available = open.filter((t) => !blocked.has(t.requesterId)).map((t) => ({ ...t, id: t._id }));
  res.json({ success: true, data: available });
});

app.get('/api/tasks/my-deliveries/:userId', authenticateToken, async (req, res) => {
  if (String(req.params.userId) !== String(req.user.id)) {
    return res.status(403).json({ success: false, error: 'Not allowed' });
  }
  const dayAgo = Date.now() - 24 * 60 * 60 * 1000;
  const rows = await C.tasks
    .find({ runnerId: req.user.id, status: { $ne: 'DELIVERED' } })
    .sort({ createdAt: -1 })
    .toArray();
  const recent = rows.filter((t) => t.status !== 'CANCELLED' || new Date(t.cancelledAt || t.createdAt).getTime() > dayAgo);
  res.json({ success: true, data: await decorateTasks(recent, req.user.id) });
});

app.get('/api/tasks/my-history/:userId', authenticateToken, async (req, res) => {
  if (String(req.params.userId) !== String(req.user.id)) {
    return res.status(403).json({ success: false, error: 'Not allowed' });
  }
  const rows = await C.tasks.find({ runnerId: req.user.id, status: 'DELIVERED' }).toArray();
  const when = (t) => new Date(t.deliveredAt || t.createdAt).getTime();
  rows.sort((a, b) => when(b) - when(a));

  const { dayStart, weekStart, monthStart } = periodStarts();
  const sums = {
    today: { earned: 0, jobs: 0 },
    week: { earned: 0, jobs: 0 },
    month: { earned: 0, jobs: 0 },
    all: { earned: 0, jobs: 0 },
  };
  rows.forEach((t) => {
    const fee = Number(t.deliveryFee) || 0;
    const time = when(t);
    const add = (bucket) => { bucket.earned += fee; bucket.jobs += 1; };
    add(sums.all);
    if (time >= monthStart) add(sums.month);
    if (time >= weekStart) add(sums.week);
    if (time >= dayStart) add(sums.today);
  });
  Object.values(sums).forEach((bucket) => { bucket.earned = round2(bucket.earned); });

  const jobs = await decorateTasks(rows.slice(0, 100), req.user.id);
  res.json({ success: true, data: { summary: sums, jobs } });
});

app.get('/api/tasks/my-orders/:userId', authenticateToken, async (req, res) => {
  if (String(req.params.userId) !== String(req.user.id)) {
    return res.status(403).json({ success: false, error: 'Not allowed' });
  }
  const rows = await C.tasks.find({ requesterId: req.user.id }).sort({ createdAt: -1 }).limit(100).toArray();
  res.json({ success: true, data: await decorateTasks(rows, req.user.id) });
});

app.post('/api/tasks', authenticateToken, requireVerified, rateLimit('create-task', 60 * 60 * 1000, 20, byUser), async (req, res) => {
  const restaurantName = cleanStr(req.body?.restaurantName, 80);
  const pickupAddress = cleanStr(req.body?.pickupAddress, 120);
  const dropoffAddress = cleanStr(req.body?.dropoffAddress, 120);
  const orderDetails = cleanStr(req.body?.orderDetails, 1000);
  if (!restaurantName || !pickupAddress || !dropoffAddress || !orderDetails) {
    return res.status(400).json({ success: false, error: 'Please fill in all order details' });
  }

  const activeOrder = await C.tasks.findOne({ requesterId: req.user.id, status: { $in: ACTIVE_STATUSES } });
  if (activeOrder) {
    return res.status(409).json({ success: false, error: 'You already have an active order. Please wait until it is delivered or cancel it first.' });
  }

  const plates = parsePlates(orderDetails, req.body?.totalItems);
  const minFee = minimumFee(pickupAddress, plates);
  const fee = Number(req.body?.deliveryFee);
  if (!Number.isFinite(fee)) return res.status(400).json({ success: false, error: 'Invalid runner fee' });
  if (fee < minFee) return res.status(400).json({ success: false, error: `The runner fee must be at least P${minFee} for this order.` });
  if (fee > MAX_FEE) return res.status(400).json({ success: false, error: `The runner fee cannot be more than P${MAX_FEE}.` });

  const newTask = {
    _id: newId('task'),
    restaurantName,
    pickupAddress,
    dropoffAddress,
    orderDetails,
    deliveryFee: Math.round(fee * 100) / 100,
    estimatedTime: Number(req.body?.estimatedTime) || 30,
    totalItems: plates,
    scheduledDeliveryAt: req.body?.scheduledDeliveryAt ? cleanStr(req.body.scheduledDeliveryAt, 60) : null,
    status: 'OPEN',
    requesterId: req.user.id,
    runnerId: null,
    createdAt: nowIso(),
  };
  await C.tasks.insertOne(newTask);

  const runnerIds = await runnerIdsFor(req.user.id);
  const payload = { ...newTask, id: newTask._id };
  emitToUsers([req.user.id, ...runnerIds], 'new_order', payload);

  sendPushToUsers(runnerIds, {
    title: 'New order available 🍔',
    body: `${newTask.restaurantName} → ${newTask.dropoffAddress} · P${Number(newTask.deliveryFee).toFixed(2)}`,
    data: { type: 'new_order', taskId: newTask._id },
  });

  res.status(201).json({ success: true, data: await decorateTask(newTask, req.user.id) });
});

app.post('/api/tasks/:taskId/claim', authenticateToken, requireVerified, async (req, res) => {
  const task = await C.tasks.findOne({ _id: req.params.taskId });
  if (!task) return res.status(404).json({ success: false, error: 'Task not found' });
  if (req.user.role !== 'RUNNER') return res.status(403).json({ success: false, error: 'Only runners can claim orders' });
  if (task.requesterId === req.user.id) return res.status(403).json({ success: false, error: 'You cannot claim your own order' });
  if (task.status !== 'OPEN') return res.status(409).json({ success: false, error: 'This order has already been claimed' });
  if (await isBlocked(req.user.id, task.requesterId)) return res.status(403).json({ success: false, error: 'You cannot claim this order' });

  const activeCount = await C.tasks.countDocuments({ runnerId: req.user.id, status: { $in: CHAT_STATUSES } });
  if (activeCount >= MAX_ACTIVE_DELIVERIES) {
    return res.status(409).json({ success: false, error: `You already have ${MAX_ACTIVE_DELIVERIES} active deliveries. Finish one first.` });
  }

  const claimed = await C.tasks.findOneAndUpdate(
    { _id: task._id, status: 'OPEN' },
    { $set: { status: 'CLAIMED', runnerId: req.user.id, claimedAt: nowIso() } },
    { returnDocument: 'after' }
  );
  if (!claimed) return res.status(409).json({ success: false, error: 'This order has already been claimed' });

  emitToUsers(await runnerIdsFor(claimed.requesterId), 'order_claimed', { taskId: claimed._id });
  await broadcastStatus(claimed);
  sendPushToUsers([claimed.requesterId], {
    title: 'Order claimed ✅',
    body: `${req.user.name} has claimed your order!`,
    data: { type: 'order_claimed', taskId: claimed._id },
  });

  await createNotification({
    userId: claimed.requesterId,
    type: 'order_claimed',
    title: 'Order claimed ✅',
    body: `${req.user.name} has claimed your order from ${claimed.restaurantName}.`,
    taskId: claimed._id,
  });

  res.json({ success: true, data: await decorateTask(claimed, req.user.id) });
});

app.patch('/api/tasks/:taskId/status', authenticateToken, async (req, res) => {
  const task = await C.tasks.findOne({ _id: req.params.taskId });
  if (!task) return res.status(404).json({ success: false, error: 'Task not found' });
  if (String(task.runnerId) !== String(req.user.id)) {
    return res.status(403).json({ success: false, error: 'Only the assigned runner can update this order' });
  }
  const status = req.body?.status;
  if (!['PICKED_UP', 'ON_THE_WAY', 'DELIVERED'].includes(status)) {
    return res.status(400).json({ success: false, error: 'Invalid status' });
  }
  if (NEXT_STATUS[task.status] !== status) {
    return res.status(409).json({ success: false, error: `This order is ${task.status.replace(/_/g, ' ').toLowerCase()} and cannot be changed to that.` });
  }

  const updated = await C.tasks.findOneAndUpdate(
    { _id: task._id, runnerId: req.user.id, status: task.status },
    { $set: { status, [STATUS_TIME_FIELD[status]]: nowIso() } },
    { returnDocument: 'after' }
  );
  if (!updated) return res.status(409).json({ success: false, error: 'This order was just changed. Please refresh.' });

  await broadcastStatus(updated);

  const statusPushText = {
    PICKED_UP: { title: 'Order picked up 📦', body: 'Your order has been picked up!' },
    ON_THE_WAY: { title: 'On the way 🚴', body: 'Your runner is on the way to you!' },
    DELIVERED: { title: 'Order delivered 🎉', body: 'Your order has been delivered. Enjoy!' },
  };
  sendPushToUsers([updated.requesterId], {
    ...statusPushText[status],
    data: { type: 'status_updated', taskId: updated._id, status },
  });

  await createNotification({
    userId: updated.requesterId,
    type: `order_${status.toLowerCase()}`,
    title: statusPushText[status].title,
    body: statusPushText[status].body,
    taskId: updated._id,
  });

  res.json({ success: true, data: await decorateTask(updated, req.user.id) });
});

app.patch('/api/tasks/:taskId/cancel', authenticateToken, async (req, res) => {
  const task = await C.tasks.findOne({ _id: req.params.taskId });
  if (!task) return res.status(404).json({ success: false, error: 'Task not found' });
  if (task.requesterId !== req.user.id) {
    return res.status(403).json({ success: false, error: 'Only the order requester can cancel' });
  }
  if (task.status === 'CANCELLED') return res.status(400).json({ success: false, error: 'This order is already cancelled' });
  if (task.status === 'DELIVERED') return res.status(400).json({ success: false, error: 'Cannot cancel a delivered order' });
  if (task.status === 'PICKED_UP' || task.status === 'ON_THE_WAY') {
    return res.status(400).json({ success: false, error: 'Your runner has already picked up your order. Please talk to them in chat.' });
  }

  const previousStatus = task.status;
  const cancelled = await C.tasks.findOneAndUpdate(
    { _id: task._id, status: previousStatus },
    { $set: { status: 'CANCELLED', cancelledAt: nowIso(), cancelledBy: req.user.id } },
    { returnDocument: 'after' }
  );
  if (!cancelled) {
    return res.status(409).json({ success: false, error: 'This order was just updated. Please refresh and try again.' });
  }
  if (previousStatus === 'CLAIMED') await C.users.updateOne({ _id: req.user.id }, { $inc: { lateCancels: 1 } });

  if (previousStatus === 'OPEN') emitToUsers(await runnerIdsFor(cancelled.requesterId), 'order_cancelled', { taskId: cancelled._id });
  await broadcastStatus(cancelled);

  if (cancelled.runnerId) {
    sendPushToUsers([cancelled.runnerId], {
      title: 'Order Cancelled ❌',
      body: `The customer cancelled their order from ${cancelled.restaurantName || 'a restaurant'}.`,
      data: { type: 'order_cancelled', taskId: cancelled._id },
    });

    await createNotification({
      userId: cancelled.runnerId,
      type: 'order_cancelled',
      title: 'Order cancelled ❌',
      body: `The customer cancelled the order from ${cancelled.restaurantName}.`,
      taskId: cancelled._id,
    });
  }
  res.json({ success: true, data: await decorateTask(cancelled, req.user.id) });
});

const startExpiryJob = () => {
  setInterval(async () => {
    try {
      const open = await C.tasks.find({ status: 'OPEN' }).toArray();
      for (const task of open) {
        const scheduled = /^scheduled:/im.test(task.orderDetails || '');
        const expiryMs = scheduled ? SCHEDULED_EXPIRY_HOURS * 3600 * 1000 : ORDER_EXPIRY_MINUTES * 60 * 1000;
        if (Date.now() - new Date(task.createdAt).getTime() < expiryMs) continue;

        const expired = await C.tasks.findOneAndUpdate(
          { _id: task._id, status: 'OPEN' },
          { $set: { status: 'CANCELLED', cancelledAt: nowIso(), cancelledBy: 'system', cancelReason: 'NO_RUNNER' } },
          { returnDocument: 'after' }
        );
        if (!expired) continue;

        emitToUsers(await runnerIdsFor(expired.requesterId), 'order_cancelled', { taskId: expired._id });
        await broadcastStatus(expired);
        sendPushToUsers([expired.requesterId], {
          title: 'No runner found',
          body: 'We could not find a runner for your order. You can place it again, maybe with a higher offer.',
          data: { type: 'order_cancelled', taskId: expired._id },
        });

        await createNotification({
          userId: expired.requesterId,
          type: 'order_cancelled',
          title: 'No runner found',
          body: 'We could not find a runner for your order.',
          taskId: expired._id,
        });
      }
    } catch (err) {
      console.error('Order expiry job failed:', err.message);
    }
  }, 60 * 1000).unref();
};

// ==================== RATINGS ====================
app.post('/api/reviews', authenticateToken, rateLimit('review', 60 * 60 * 1000, 30, byUser), async (req, res) => {
  const { taskId, rating, comment } = req.body || {};
  const task = await C.tasks.findOne({ _id: String(taskId) });
  if (!task) return res.status(404).json({ success: false, error: 'Order not found' });
  if (!isTaskParticipant(task, req.user.id)) return res.status(403).json({ success: false, error: 'You are not part of this order' });
  if (task.status !== 'DELIVERED') return res.status(400).json({ success: false, error: 'You can only rate delivered orders' });

  const stars = parseInt(rating, 10);
  if (!(stars >= 1 && stars <= 5)) return res.status(400).json({ success: false, error: 'Rating must be between 1 and 5' });

  const targetId = req.user.id === task.requesterId ? task.runnerId : task.requesterId;
  const id = newId('rev');
  const review = {
    _id: id,
    id,
    taskId: task._id,
    authorId: req.user.id,
    targetId,
    rating: stars,
    comment: cleanStr(comment, 300) || null,
    createdAt: nowIso(),
  };
  try {
    await C.reviews.insertOne(review);
  } catch (err) {
    if (isDuplicateKey(err)) return res.status(409).json({ success: false, error: 'You already rated this order' });
    throw err;
  }

  await createNotification({
    userId: targetId,
    type: 'new_review',
    title: `New ${stars}-star review ⭐`,
    body: review.comment ? review.comment : `${req.user.name} rated you ${stars} stars.`,
    taskId: task._id,
  });

  const { _id, ...data } = review;
  res.status(201).json({ success: true, data });
});

app.get('/api/users/:userId/stats', authenticateToken, async (req, res) => {
  const userId = String(req.params.userId);
  const [delivered, active, agg] = await Promise.all([
    C.tasks.countDocuments({ runnerId: userId, status: 'DELIVERED' }),
    C.tasks.countDocuments({ runnerId: userId, status: { $in: CHAT_STATUSES } }),
    C.reviews.aggregate([
      { $match: { targetId: userId } },
      { $group: { _id: null, avg: { $avg: '$rating' }, n: { $sum: 1 } } },
    ]).toArray(),
  ]);
  const rating = agg.length ? Math.round(agg[0].avg * 10) / 10 : null;
  res.json({
    success: true,
    data: {
      rating,
      ratingCount: agg.length ? agg[0].n : 0,
      totalOrders: delivered,
      activeDeliveries: active,
      completedDeliveries: delivered,
    },
  });
});

app.get('/api/users/me/reviews', authenticateToken, async (req, res) => {
  const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 20, 1), 100);
  const skip = Math.max(parseInt(req.query.skip, 10) || 0, 0);

  const [rows, total] = await Promise.all([
    C.reviews.find({ authorId: req.user.id }).sort({ createdAt: -1 }).skip(skip).limit(limit).toArray(),
    C.reviews.countDocuments({ authorId: req.user.id }),
  ]);

  const targetIds = [...new Set(rows.map((r) => r.targetId))];
  const targets = await usersByIds(targetIds);

  const data = rows.map((r) => {
    const { _id, ...rest } = r;
    return { ...rest, target: minimalUser(targets.get(r.targetId)) };
  });

  res.json({ success: true, data: { reviews: data, total, hasMore: skip + data.length < total } });
});

app.get('/api/users/:userId/reviews', authenticateToken, async (req, res) => {
  const userId = String(req.params.userId);
  const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 20, 1), 100);
  const skip = Math.max(parseInt(req.query.skip, 10) || 0, 0);

  if (await isBlocked(req.user.id, userId)) {
    return res.status(403).json({ success: false, error: 'You cannot view this profile' });
  }

  const [rows, total] = await Promise.all([
    C.reviews
      .find({ targetId: userId })
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(limit)
      .toArray(),
    C.reviews.countDocuments({ targetId: userId }),
  ]);

  const authorIds = [...new Set(rows.map((r) => r.authorId))];
  const taskIds = [...new Set(rows.map((r) => r.taskId))];
  const [authors, tasks] = await Promise.all([
    usersByIds(authorIds),
    taskIds.length
      ? C.tasks.find({ _id: { $in: taskIds } }, { projection: { restaurantName: 1, createdAt: 1 } }).toArray()
      : Promise.resolve([]),
  ]);
  const taskMap = new Map(tasks.map((t) => [t._id, t]));

  const data = rows.map((r) => {
    const { _id, ...rest } = r;
    const author = authors.get(r.authorId);
    const task = taskMap.get(r.taskId);
    return {
      ...rest,
      author: author ? minimalUser(author) : { id: r.authorId, name: 'Deleted user', profilePicture: null, role: null },
      task: task ? { id: task._id, restaurantName: task.restaurantName, createdAt: task.createdAt } : null,
      isMine: r.authorId === req.user.id,
    };
  });

  res.json({
    success: true,
    data: {
      reviews: data,
      total,
      hasMore: skip + data.length < total,
    },
  });
});

// ==================== REPORTS & BLOCKS ====================
app.post('/api/reports', authenticateToken, rateLimit('report', 60 * 60 * 1000, 10, byUser), async (req, res) => {
  const { targetUserId, taskId, reason, details } = req.body || {};
  const target = await findUserById(targetUserId);
  if (!target) return res.status(404).json({ success: false, error: 'User not found' });
  if (target.id === req.user.id) return res.status(400).json({ success: false, error: 'You cannot report yourself' });
  if (!REPORT_REASONS.includes(reason)) return res.status(400).json({ success: false, error: 'Please choose a reason' });
  if (taskId) {
    const task = await C.tasks.findOne({ _id: String(taskId) });
    if (!task || !isTaskParticipant(task, req.user.id)) {
      return res.status(403).json({ success: false, error: 'You are not part of this order' });
    }
  }
  const id = newId('rep');
  await C.reports.insertOne({
    _id: id,
    id,
    reporterId: req.user.id,
    targetUserId: target.id,
    taskId: taskId || null,
    reason,
    details: cleanStr(details, 500),
    status: 'OPEN',
    createdAt: nowIso(),
  });
  console.log(`🚩 Report ${id}: ${req.user.id} reported ${target.id} (${reason})`);
  res.status(201).json({ success: true });
});

app.get('/api/blocks', authenticateToken, async (req, res) => {
  const rows = await C.blocks.find({ blockerId: req.user.id }).toArray();
  const users = await usersByIds(rows.map((b) => b.blockedId));
  const list = rows.map((b) => ({ userId: b.blockedId, name: users.get(b.blockedId)?.name || 'User', createdAt: b.createdAt }));
  res.json({ success: true, data: list });
});

app.post('/api/blocks', authenticateToken, async (req, res) => {
  const userId = String(req.body?.userId || '');
  if (!(await findUserById(userId))) return res.status(404).json({ success: false, error: 'User not found' });
  if (userId === req.user.id) return res.status(400).json({ success: false, error: 'You cannot block yourself' });
  await C.blocks.updateOne(
    { blockerId: req.user.id, blockedId: userId },
    { $setOnInsert: { createdAt: nowIso() } },
    { upsert: true }
  );
  res.json({ success: true });
});

app.delete('/api/blocks/:userId', authenticateToken, async (req, res) => {
  await C.blocks.deleteMany({ blockerId: req.user.id, blockedId: req.params.userId });
  res.json({ success: true });
});

// ==================== ADMIN ====================
app.get('/api/admin/reports', authenticateToken, requireAdmin, async (req, res) => {
  const rows = await C.reports.find({}).sort({ createdAt: -1 }).toArray();
  const users = await usersByIds([...new Set(rows.flatMap((r) => [r.reporterId, r.targetUserId]))]);
  const list = rows.map((r) => {
    const { _id, ...rest } = r;
    const reporter = users.get(r.reporterId);
    const target = users.get(r.targetUserId);
    return {
      ...rest,
      reporter: reporter ? { id: reporter.id, name: reporter.name, email: reporter.email } : null,
      target: target ? { id: target.id, name: target.name, email: target.email, suspended: !!target.suspended } : null,
    };
  });
  res.json({ success: true, data: list });
});

app.patch('/api/admin/reports/:reportId', authenticateToken, requireAdmin, async (req, res) => {
  const status = req.body?.status === 'OPEN' ? 'OPEN' : 'RESOLVED';
  const report = await C.reports.findOneAndUpdate(
    { _id: req.params.reportId },
    { $set: { status } },
    { returnDocument: 'after' }
  );
  if (!report) return res.status(404).json({ success: false, error: 'Report not found' });
  const { _id, ...data } = report;
  res.json({ success: true, data });
});

app.post('/api/admin/users/:userId/suspend', authenticateToken, requireAdmin, async (req, res) => {
  const user = await findUserById(req.params.userId);
  if (!user) return res.status(404).json({ success: false, error: 'User not found' });
  const suspend = req.body?.suspended !== false;
  await C.users.updateOne({ _id: user.id }, { $set: { suspended: suspend } });
  if (suspend) {
    await Promise.all([deleteUserSessions(user.id), C.pushTokens.deleteMany({ userId: user.id })]);
  }
  res.json({ success: true, data: { id: user.id, suspended: suspend } });
});

// ==================== SAVED LOCATIONS ====================
app.get('/api/saved-locations/:userId', authenticateToken, async (req, res) => {
  if (String(req.params.userId) !== String(req.user.id)) {
    return res.status(403).json({ success: false, error: 'Not allowed' });
  }
  const rows = await C.locations.find({ userId: req.user.id }).toArray();
  res.json({ success: true, data: rows.map(({ _id, ...rest }) => rest) });
});

app.post('/api/saved-locations', authenticateToken, async (req, res) => {
  const label = cleanStr(req.body?.label, 40);
  const address = cleanStr(req.body?.address, 120);
  if (!label || !address) return res.status(400).json({ success: false, error: 'Label and address are required' });
  const userId = req.user.id;
  const id = newId('loc');
  const newLocation = {
    id,
    userId,
    label,
    address,
    isDefault: (await C.locations.countDocuments({ userId })) === 0,
  };
  await C.locations.insertOne({ _id: id, ...newLocation });
  res.status(201).json({ success: true, data: newLocation });
});

// ==================== CHAT ====================
async function notifyChatRecipient(message, previewText) {
  const task = await C.tasks.findOne({ _id: message.taskId });
  if (!task) return;
  const senderId = String(message.senderId || '');
  const recipientId = senderId === String(task.requesterId) ? task.runnerId : task.requesterId;
  if (!recipientId || String(recipientId) === senderId) return;
  await sendPushToUsers([recipientId], {
    title: message.senderName || 'New message',
    body: previewText,
    data: { type: 'new_message', taskId: message.taskId },
  });
}

const chatOpenFor = async (task) =>
  !!task && CHAT_STATUSES.includes(task.status) && !(await isBlocked(task.requesterId, task.runnerId));

app.get('/api/messages/:taskId', authenticateToken, async (req, res) => {
  const { taskId } = req.params;
  const task = await C.tasks.findOne({ _id: taskId });
  if (!task) return res.status(404).json({ success: false, error: 'Order not found' });
  if (!isTaskParticipant(task, req.user.id)) {
    return res.status(403).json({ success: false, error: 'You are not part of this order' });
  }
  const rows = await C.messages.find({ taskId }).sort({ createdAt: 1 }).toArray();
  res.json({ success: true, data: rows });
});

app.post('/api/messages', authenticateToken, rateLimit('message', 60 * 1000, 60, byUser), async (req, res) => {
  const { taskId } = req.body || {};
  const content = cleanStr(req.body?.content, 1000);
  const task = await C.tasks.findOne({ _id: String(taskId) });
  if (!task) return res.status(404).json({ success: false, error: 'Order not found' });
  if (!isTaskParticipant(task, req.user.id)) {
    return res.status(403).json({ success: false, error: 'You are not part of this order' });
  }
  if (!(await chatOpenFor(task))) {
    return res.status(409).json({ success: false, error: 'Chat is only available while a runner is handling this order' });
  }
  if (!content) return res.status(400).json({ success: false, error: 'Message cannot be empty' });

  const newMessage = {
    _id: crypto.randomUUID(),
    taskId: task._id,
    senderId: req.user.id,
    senderName: req.user.name,
    senderProfilePicture: req.user.profilePicture || null,
    content,
    type: 'text',
    createdAt: nowIso(),
  };
  await C.messages.insertOne(newMessage);
  emitToUsers([task.requesterId, task.runnerId], 'new_message', newMessage);
  notifyChatRecipient(newMessage, content.length > 100 ? `${content.slice(0, 100)}…` : content);

  const recipientId = String(req.user.id) === String(task.requesterId) ? task.runnerId : task.requesterId;
  if (recipientId) {
    await createNotification({
      userId: recipientId,
      type: 'new_message',
      title: `New message from ${req.user.name}`,
      body: content.length > 80 ? `${content.slice(0, 80)}…` : content,
      taskId: task._id,
    });
  }

  res.status(201).json({ success: true, data: newMessage });
});

const audioFilter = (req, file, cb) => {
  if (file.mimetype.startsWith('audio/') || file.originalname.match(/\.(mp3|m4a|wav|webm|ogg|aac)$/i)) cb(null, true);
  else cb(new Error('Only audio files are allowed!'));
};
const uploadVoice = multer({
  storage: multer.memoryStorage(),
  fileFilter: audioFilter,
  limits: { fileSize: 15 * 1024 * 1024 },
});

app.post('/api/messages/voice', authenticateToken, rateLimit('voice', 60 * 1000, 20, byUser), uploadVoice.single('audio'), async (req, res) => {
  try {
    const { taskId, duration } = req.body || {};
    if (!req.file) return res.status(400).json({ success: false, error: 'No audio file provided' });

    const task = await C.tasks.findOne({ _id: String(taskId) });
    if (!task || !isTaskParticipant(task, req.user.id)) {
      return res.status(403).json({ success: false, error: 'You are not part of this order' });
    }
    if (!(await chatOpenFor(task))) {
      return res.status(409).json({ success: false, error: 'Chat is only available while a runner is handling this order' });
    }

    let audioUrl;
    if (CLOUD_ENABLED) {
      const result = await uploadToCloud(req.file.buffer, { folder: 'campus-eats/voices', resource_type: 'video' });
      audioUrl = result.secure_url;
    } else {
      const ext = path.extname(req.file.originalname).toLowerCase().replace(/[^.a-z0-9]/g, '').slice(0, 6);
      const filename = `${Date.now()}-${crypto.randomBytes(6).toString('hex')}${ext}`;
      fs.writeFileSync(path.join(voicesDir, filename), req.file.buffer);
      audioUrl = `${baseUrl(req)}/uploads/voices/${filename}`;
    }

    const newMessage = {
      _id: crypto.randomUUID(),
      taskId: task._id,
      senderId: req.user.id,
      senderName: req.user.name,
      senderProfilePicture: req.user.profilePicture || null,
      content: '',
      type: 'voice',
      audioUrl,
      duration: Math.min(parseInt(duration, 10) || 0, 600),
      createdAt: nowIso(),
    };
    await C.messages.insertOne(newMessage);
    emitToUsers([task.requesterId, task.runnerId], 'new_message', newMessage);
    notifyChatRecipient(newMessage, '🎤 Sent you a voice message');

    const recipientId = String(req.user.id) === String(task.requesterId) ? task.runnerId : task.requesterId;
    if (recipientId) {
      await createNotification({
        userId: recipientId,
        type: 'new_message',
        title: `New message from ${req.user.name}`,
        body: '🎤 Sent you a voice message',
        taskId: task._id,
      });
    }

    res.status(201).json({ success: true, data: newMessage });
  } catch (err) {
    console.error('Voice upload error:', err);
    res.status(500).json({ success: false, error: 'Failed to upload voice note' });
  }
});

// ==================== ERRORS ====================
app.use((req, res) => res.status(404).json({ success: false, error: 'Not found' }));

app.use((err, req, res, next) => {
  if (err && err.type === 'entity.too.large') return res.status(413).json({ success: false, error: 'That upload is too large' });
  if (err instanceof multer.MulterError) return res.status(400).json({ success: false, error: err.message });
  if (err && err.type === 'entity.parse.failed') return res.status(400).json({ success: false, error: 'Invalid request' });
  console.error('Unhandled error:', err);
  const status = err && err.status && err.status < 500 ? err.status : 500;
  res.status(status).json({ success: false, error: status === 500 ? 'Server error' : err.message });
});

// ==================== START SERVER ====================
const start = async () => {
  await connectDb();
  startExpiryJob();
  server.listen(PORT, '0.0.0.0', () => {
    console.log(`✅ Server running on port ${PORT}`);
    console.log(`🍃 Database: MongoDB (${MONGODB_DB})`);
    console.log(`🖼️  File storage: ${CLOUD_ENABLED ? 'Cloudinary' : 'local disk'}`);
    console.log(`✉️  Email: ${mailer ? 'SMTP configured' : 'not configured'}`);
    console.log(`🔑 Google sign-in: ${googleClient ? 'enabled' : 'not configured'}`);
    console.log(`🔐 Email verification enforced: ${ENFORCE_EMAIL_VERIFICATION}`);
    console.log(`🔔 Push notifications: enabled`);
  });
};

start().catch((err) => {
  console.error('❌ Failed to start server:', err.message);
  process.exit(1);
});