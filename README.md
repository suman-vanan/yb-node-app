# YugabyteDB Node.js Sample App

## What does this app do?

This demo app is intended to showcase YugabyteDB's __reseliency__ capabilities. The app runs a load test for 10 mins, during which DB nodes can be unexpectedly brought down with limited impact to the app.

The following are the key ingredients for this demo:
- Use [YugabyteDB smart drivers for YSQL](https://docs.yugabyte.com/stable/develop/drivers-orms/smart-drivers/) for automatic uniform connection load balancing
- Use client-side retry logic for in-flight transactions to an unreachable DB node (see docs on [HA transactions](https://docs.yugabyte.com/stable/explore/fault-tolerance/transaction-availability/))

## How to run app

```bash
npm install

# Set the relevant environment variables and run app
DB_NAME=demo_db NODE1_DB_HOST=10.98.57.116 NODE2_DB_HOST=10.98.59.243 NODE3_DB_HOST=10.98.61.22 node index.js
```