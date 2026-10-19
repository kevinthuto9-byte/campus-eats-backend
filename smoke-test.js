// smoke-test.js — Run with: npm run smoke
require('dotenv').config();

const BASE = `http://localhost:${process.env.PORT || 4000}`;

const check = async (name, fn) => {
  try {
    await fn();
    console.log(`✅ ${name}`);
  } catch (err) {
    console.error(`❌ ${name} — ${err.message}`);
  }
};

const assert = (cond, msg) => {
  if (!cond) throw new Error(msg);
};

const get = async (path) => {
  const res = await fetch(`${BASE}${path}`);
  return { status: res.status, body: await res.json().catch(() => null) };
};

const post = async (path, body, headers = {}) => {
  const res = await fetch(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...headers },
    body: JSON.stringify(body),
  });
  return { status: res.status, body: await res.json().catch(() => null) };
};

(async () => {
  console.log(`\n🔍 Running smoke tests against ${BASE}\n`);

  await check('Server is up (GET /)', async () => {
    const { status, body } = await get('/');
    assert(status === 200, `Expected 200, got ${status}`);
    assert(body.status === 'Server is running!', 'Unexpected body');
  });

  await check('Health endpoint (GET /health)', async () => {
    const { status, body } = await get('/health');
    assert(status === 200, `Expected 200, got ${status}`);
    assert(body.ok === true, 'Health check failed');
  });

  await check('Signup validation rejects short password', async () => {
    const { status } = await post('/api/auth/signup', {
      name: 'Test User',
      email: `smoke_${Date.now()}@test.com`,
      password: '123',
    });
    assert(status === 400, `Expected 400, got ${status}`);
  });

  await check('Signup creates a user', async () => {
    const email = `smoke_${Date.now()}@test.com`;
    const { status, body } = await post('/api/auth/signup', {
      name: 'Smoke Tester',
      email,
      password: 'correcthorsebattery',
      role: 'CUSTOMER',
    });
    assert(status === 201, `Expected 201, got ${status}`);
    assert(body.data?.token, 'No token returned');
    assert(body.data?.user?.email === email, 'Email mismatch');
  });

  await check('Login rejects bad password', async () => {
    const { status } = await post('/api/auth/login', {
      email: 'nobody@nowhere.com',
      password: 'wrong',
    });
    assert(status === 401, `Expected 401, got ${status}`);
  });

  await check('Protected route rejects missing token', async () => {
    const { status } = await get('/api/tasks/available');
    assert(status === 401, `Expected 401, got ${status}`);
  });

  console.log('\n✅ Smoke tests complete.\n');
})();