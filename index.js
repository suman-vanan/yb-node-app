const { Pool } = require('@yugabytedb/pg'); // https://github.com/yugabyte/node-postgres 

// Configure the base settings shared across all nodes
const baseConfig = {
  user: process.env.DB_USER || 'yugabyte',
  database: process.env.DB_NAME || 'my_database',
  password: process.env.DB_PASSWORD || 'password',
  // To enable the cluster-aware connection load balancing, provide the parameter loadBalance set to true
  loadBalance: true,
  ybServersRefreshInterval: 300,
  // CRITICAL: Set a timeout. If the host goes down (black holes traffic), 
  // we don't want to wait infinitely. 3 seconds is usually safe.
  connectionTimeoutMillis: 3000,
  // Ignore TLS verification (useful for self-signed certificates or internal networks)
  ssl: {
    rejectUnauthorized: false
  },
  // Configure the number of clients in the pool
  min: process.env.DB_POOL_MIN || 10,
  max: process.env.DB_POOL_MAX || 20,
};

// Configure the 3 DB nodes in order of preference
// Initial connection will use the first available DB node
// When `loadBalance` is enabled, pool will automatically load balance connections amongst DB nodes
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

class DatabaseManager {
  constructor() {
    this.pool = null;
  }

  /**
   * Helper to create a new pool and attach error handlers
   */
  createPool(nodeIndex) {
    const config = nodesConfigs[nodeIndex];
    const pool = new Pool(config);
    const nodeName = `node${nodeIndex + 1}`;

     // Prevent Node.js from crashing if an idle client in the pool errors out
    pool.on('error', (err) => {
      console.error(`⚠️ Unexpected error on ${nodeName} pool idle client:`, err.message);
    });
    
    // Prevent Node.js from crashing if actively checked-out clients (or background driver clients) error out
    pool.on('connect', (client) => {
      client.on('error', (err) => {
        console.error(`⚠️ Active/Background client error on ${nodeName}:`, err.message);
      });
    });
    
    return pool;
  }

  /**
   * Helper to get a client from the pool, ensuring the first connection is healthy
   */
  async getClient() {
    // 1. Initialize the pool on the first call, finding a healthy seed node
    if (!this.pool) {
      let initialized = false;

      for (let i = 0; i < nodesConfigs.length; i++) {
        console.log(`[INIT] Attempting to initialize pool with node${i + 1}...`);
        const candidatePool = this.createPool(i);
        
        try {
          // Test the connection
          console.log(`[INIT] testing connection`)
          const client = await candidatePool.connect();
          client.release(); // Success! Host is reachable.
          
          this.pool = candidatePool;
          initialized = true;
          console.log(`[INIT] Pool successfully initialized using node${i + 1} as seed.`);
          break;
        } catch (err) {
          console.warn(`[INIT] node${i + 1} connection failed: ${err.message}`);
          // Clean up the candidate pool before trying the next one
          await candidatePool.end().catch(cleanupErr => {
            console.warn(`[INIT] Non-fatal error while ending candidate pool: ${cleanupErr.message}`);
          });
        }
      }

      if (!initialized) {
        const fatalMsg = '❌ FATAL: All database nodes are unreachable.';
        console.error(fatalMsg);
        throw new Error(fatalMsg);
      }
    }
    
    // 2. Subsequent connections rely on the driver's built-in load balancing
    return await this.pool.connect();
  }

  /**
   * Smart query method for simple single-query executions
   */
  async query(text, params) {
    const client = await this.getClient();
    try {
      return await client.query(text, params);
    } finally {
      client.release();
    }
  }

  /**
   * Gracefully close the active pool when shutting down the app
   */
  async close() {
    if (this.pool) {
      await this.pool.end();
    }
  }
}

// --- Usage Example ---
async function main() {
  const db = new DatabaseManager();

  try {
    const maxClients = parseInt(process.env.DB_POOL_MAX || 20, 10);
    const testDurationSec = parseInt(process.env.LOAD_TEST_DURATION_SEC || 300, 10);
    
    console.log(`[LOAD TEST] Setting up table...`);
    await db.query(`
      CREATE TABLE IF NOT EXISTS public.postgresqlkeyvalue (
          k text NOT NULL,
          v text,
          CONSTRAINT postgresqlkeyvalue_pkey PRIMARY KEY((k) HASH)
      ) SPLIT INTO 6 TABLETS;
    `);
    
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
          // A single query is implicitly a transaction in Postgres.
          // Using the db.query helper handles checkout, execution, and release automatically.
          await db.query('INSERT INTO PostgresqlKeyValue (k, v) VALUES ($1, $2)', [key, value]);
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
    await db.close();
  }
}

// Run the example if this file is executed directly
if (require.main === module) {
  main();
}

module.exports = DatabaseManager;