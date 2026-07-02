const { Pool } = require('@yugabytedb/pg'); // https://github.com/yugabyte/node-postgres 

// Configure the base settings shared across all nodes
// https://node-postgres.com/apis/pool
const baseConfig = {
  user: process.env.DB_USER || 'yugabyte',
  database: process.env.DB_NAME || 'my_database',
  password: process.env.DB_PASSWORD || 'password',
  // To enable the cluster-aware connection load balancing, provide the parameter loadBalance set to true
  loadBalance: true,
  ybServersRefreshInterval: 300,
  connectionTimeoutMillis: 3000,
  idleTimeoutMillis: 10000,
  max: 20,
  min: 10,
  // Ignore TLS verification (useful for self-signed certificates or internal networks)
  ssl: {
    rejectUnauthorized: false
  },
};

// Configure the 3 DB nodes in order of preference
// Initial connection will use the first available DB node
const nodesConfigs = [
  {
    ...baseConfig,
    host: process.env.NODE1_DB_HOST || 'node1-host',
    port: process.env.NODE1_DB_PORT || 5433,
  },
  {
    ...baseConfig,
    host: process.env.NODE2_DB_HOST || 'node2-host',
    port: process.env.NODE2_DB_PORT || 5433,
  },
  {
    ...baseConfig,
    host: process.env.NODE3_DB_HOST || 'node3-host',
    port: process.env.NODE3_DB_PORT || 5433,
  }
];

let pool;

function setupPool(nodeIndex) {
  const config = nodesConfigs[nodeIndex];
  const newPool = new Pool(config);
  const nodeName = `node${nodeIndex + 1}`;

  // Prevent app from crashing if an idle client in the pool disconnects or errors out
  newPool.on('error', (err) => {
    console.error(`⚠️ Unexpected error on ${nodeName} pool idle client:`, err.message);
  });

  // Prevent app from crashing if actively checked-out clients (or background driver clients) error out
  newPool.on('connect', (client) => {
    client.on('error', (err) => {
      console.error(`⚠️ Active/Background client error on ${nodeName}:`, err.message);
    });
  });

  return newPool;
}

async function initializeAndTestPool() {
  pool = setupPool(0);

  console.log('[POOL] Testing initial connection...');
  try {
    await pool.query('SELECT 1');
    console.log('[POOL] Initial connection successful.\n');
  } catch (error) {
    console.warn(`[POOL] Initial connection failed: ${error.message}`);
    console.warn('[POOL] Destroying pool and trying fallback host...\n');

    // Cleanly drain the failed pool
    await pool.end();

    // Initialize the fallback pool
    pool = setupPool(1);
    try {
      await pool.query('SELECT 1');
      console.log('[POOL] Fallback connection successful.\n');

    } catch (fallbackError) {
      console.error('[POOL] Fallback connection also failed. Aborting test.');
      throw fallbackError;
    }
  }
}

/**
 * Helper method to execute a series of queries within a transaction (https://node-postgres.com/features/transactions).
 * @param {Function} callback - An async function that takes a database client as its argument.
 * @returns The result of the callback.
 */
async function executeTransaction(callback) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await callback(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    // Always release the client back to the pool
    client.release();
  }
}


// --- Usage Example ---
async function runLoadTest() {
  try {
    const maxClients = 20; // todo: set this to the same value as pool's max connections
    const testDurationSec = parseInt(process.env.LOAD_TEST_DURATION_SEC || 600, 10);

    console.log(`[LOAD TEST] Setting up table...`);
    await executeTransaction(async (client) => {
      await client.query(`
              CREATE TABLE IF NOT EXISTS public.postgresqlkeyvalue (
                  k text NOT NULL,
                  v text,
                  CONSTRAINT postgresqlkeyvalue_pkey PRIMARY KEY((k) HASH)
              ) SPLIT INTO 6 TABLETS;
            `);
    });

    console.log(`[LOAD TEST] Starting ${maxClients} concurrent workers for ${testDurationSec} seconds...`);

    const startTime = Date.now();
    const endTime = startTime + (testDurationSec * 1000);

    // Worker function that executes queries in a time-based loop
    const runWorker = async (workerId) => {
      let insertCount = 0;

      while (Date.now() < endTime) {
        // Generate a unique key using worker ID, timestamp, and insert count
        const key = `key-w${workerId}-${Date.now()}-${insertCount}`;
        const value = `value-w${workerId}-${Math.random().toString(36).substring(2, 10)}`;

        try {
          await executeTransaction(async (client) => {
            await client.query('INSERT INTO PostgresqlKeyValue (k, v) VALUES ($1, $2)', [key, value]);
          });
          insertCount++;
        } catch (err) {
          console.error(`❌ Worker ${workerId} insert failed on iteration ${insertCount}:`, err.message);
        }
      }
      console.log(`✅ Worker ${workerId} completed ${insertCount} inserts.`);
      return insertCount;
    };

    // Spawn promises to act as concurrent "threads"
    const workers = Array.from({ length: maxClients }, (_, i) => runWorker(i + 1));

    // Wait for all workers to finish their loops and collect their insert counts
    const workerResults = await Promise.all(workers);

    const duration = (Date.now() - startTime) / 1000;
    const totalQueries = workerResults.reduce((total, count) => total + count, 0);

    console.log(`\n🎉 Load test completed!`);
    console.log(`📊 Total Queries: ${totalQueries}`);
    console.log(`⏱️  Actual Duration: ${duration.toFixed(2)} seconds`);
    console.log(`🚀 Throughput: ${(totalQueries / duration).toFixed(2)} queries/sec`);

  } catch (err) {
    console.error('Application error:', err.message);
  } finally {
    await pool.end();
  }
}

initializeAndTestPool()
  .then(runLoadTest)
  .catch(err => {
    console.error('Fatal error during load test:', err);
    process.exit(-1);
  });