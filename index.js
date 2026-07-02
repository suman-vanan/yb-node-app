const { Client, Pool } = require('@yugabytedb/pg'); // https://github.com/yugabyte/node-postgres 

// Configure the base settings shared across all nodes
const baseConfig = {
  // Client config: https://node-postgres.com/apis/client
  user: process.env.DB_USER || 'yugabyte',
  database: process.env.DB_NAME || 'my_database',
  password: process.env.DB_PASSWORD || 'password',
  statement_timeout: 3000,
  query_timeout: 3000,
  // Ignore TLS verification (useful for self-signed certificates or internal networks)
  ssl: {
    rejectUnauthorized: false
  },
  // Pool config: https://node-postgres.com/apis/pool
  connectionTimeoutMillis: 3000,
  idleTimeoutMillis: 10000,
  max: 20,
  min: 10,
  maxLifetimeSeconds: 60
};

const baseConfigWithSmartDriverParams = {
  ...baseConfig,
  loadBalance: true, // enable cluster-aware connection load balancing: driver will fetch list of tservers and distribute the connections equally across them
  ybServersRefreshInterval: 30, // time interval (in seconds) between attempts to refresh the information about cluster nodes
}

const nodesConfigs = [
  {
    ...baseConfigWithSmartDriverParams,
    host: process.env.NODE1_DB_HOST || 'node1-host',
    port: process.env.NODE1_DB_PORT || 5433,
  },
  {
    ...baseConfigWithSmartDriverParams,
    host: process.env.NODE2_DB_HOST || 'node2-host',
    port: process.env.NODE2_DB_PORT || 5433,
  },
  {
    ...baseConfigWithSmartDriverParams,
    host: process.env.NODE3_DB_HOST || 'node3-host',
    port: process.env.NODE3_DB_PORT || 5433,
  }
];

let pool;

async function checkDatabaseNodeReadiness(config) {
  // IMPORTANT: Disable load balancing for the health check.
  // This prevents the Yugabyte smart driver from attempting a topology refresh on a dead node.
  // If the first node you try is completely dead or unreachable, the smart driver fails to fetch the topology.
  const readinessCheckConfig = { ...config, loadBalance: false };
  const client = new Client(readinessCheckConfig);
  try {
    await client.connect();
    await client.query('SELECT 1');
    console.log('[INIT] Successfully connected to the database.');
    await client.end();
    return true;
  } catch (err) {
    console.error('[INIT] Database is unreachable or rejecting connections:', err.message);
    return false;
  }
}

async function initializePool() {
  for (let i = 0; i < nodesConfigs.length; i++) {
    const config = nodesConfigs[i];
    console.log(`[INIT] Checking Node ${i + 1} (${config.host}:${config.port})...`);

    const isReady = await checkDatabaseNodeReadiness(config);

    if (isReady) {
      console.log(`[INIT] Initializing Pool with Node ${i + 1} (${config.host}:${config.port})...`);
      pool = new Pool(config);

      // Prevent app from crashing if an idle client in the pool disconnects or errors out
      pool.on('error', (err) => {
        console.error(`[POOL] ⚠️ idle client disconnected:`, err.message);
      });

      // Prevent app from crashing if actively checked-out clients error out
      pool.on('connect', (client) => {
        client.on('error', (err) => {
          console.error(`[POOL] ⚠️ active client error:`, err.message);
        });
      });

      return; // Exit the function early, pool is ready
    }
  }

  console.error('[INIT] CRITICAL ERROR: All database nodes are unreachable.');
  process.exit(1);
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
          console.error(`[LOAD TEST] ❌ Worker ${workerId} insert failed on iteration ${insertCount}:`, err.message);
        }
      }
      console.log(`[LOAD TEST] ✅ Worker ${workerId} completed ${insertCount} inserts.`);
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

initializePool()
  .then(runLoadTest)
  .catch(err => {
    console.error('Fatal error during load test:', err);
    process.exit(-1);
  });