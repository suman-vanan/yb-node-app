const { Pool } = require('@yugabytedb/pg'); // https://github.com/yugabyte/node-postgres 

// Configure the base settings shared across all nodes
const baseConfig = {
  user: process.env.DB_USER || 'yugabyte',
  database: process.env.DB_NAME || 'my_database',
  password: process.env.DB_PASSWORD || 'password',
  // To enable the cluster-aware connection load balancing, provide the parameter loadBalance set to true
  loadBalance: true,
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
    this.currentNodeIndex = 0;
    this.pool = this.createPool(this.currentNodeIndex);
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
    
    return pool;
  }

  /**
   * Helper to get a client from the pool, handling failover automatically
   */
  async getClient() {
    let client;
    let connected = false;

    while (!connected) {
      try {
        client = await this.pool.connect();
        connected = true;
      } catch (err) {
        console.warn(`[FAILOVER] node${this.currentNodeIndex + 1} connection failed: ${err.message}`);

        // Failover if we have more nodes available
        if (this.currentNodeIndex < nodesConfigs.length - 1) {
          this.currentNodeIndex++;
          console.log(`[FAILOVER] Recreating pool for node${this.currentNodeIndex + 1}...`);

          // Clean up the old pool to prevent resource leaks
          try {
            await this.pool.end();
          } catch (cleanupErr) {
            console.warn(`[FAILOVER] Non-fatal error while ending pool: ${cleanupErr.message}`);
          }

          // Instantiate the new pool
          this.pool = this.createPool(this.currentNodeIndex);
        } else {
           // Exhausted all nodes
           console.error('❌ FATAL: All 3 database nodes are unreachable.');
           throw err;
        }
      }
    }
    
    client._servedBy = `node${this.currentNodeIndex + 1}`;
    return client;
  }

  /**
   * Smart query method for simple single-query executions
   */
  async query(text, params) {
    const client = await this.getClient();
    try {
      const result = await client.query(text, params);
      return {
        ...result,
        _servedBy: client._servedBy 
      };
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
    const maxClients = parseInt(process.env.DB_POOL_MAX || 20, 20);
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
        
        // Checkout a dedicated client for this transaction
        const client = await db.getClient();
        try {
          await client.query('BEGIN');
          await client.query('INSERT INTO PostgresqlKeyValue (k, v) VALUES ($1, $2)', [key, value]);
          await client.query('COMMIT');
          insertCount++;
        } catch (err) {
          await client.query('ROLLBACK');
          console.error(`❌ Worker ${workerId} transaction failed on iteration ${insertCount}:`, err.message);
        } finally {
          // ALWAYS release the client back to the pool
          client.release();
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