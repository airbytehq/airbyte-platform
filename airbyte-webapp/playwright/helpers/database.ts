/**
 * Database helpers for Playwright tests
 * Provides direct PostgreSQL access for schema manipulation tests
 *
 * WORKER ISOLATION:
 * Each worker operates in its own PostgreSQL schema (test_worker_N)
 * This allows parallel test execution without database pollution.
 */

import pgPromise from "pg-promise";

const pgp = pgPromise();

// ============================================================================
// Worker Isolation
// ============================================================================

/** Returns the PostgreSQL schema name for the current worker (test_worker_N) */
export const getWorkerSchema = (workerIndex?: number): string => {
  const index = workerIndex ?? Number(process.env.TEST_PARALLEL_INDEX ?? 0);
  return `test_worker_${index}`;
};

// ============================================================================
// Query Builders
// ============================================================================

export const createTable = (tableName: string, columns: string[], schema: string = "public"): string => {
  return `CREATE TABLE ${schema}.${tableName}(${columns.join(", ")});`;
};

export const dropTable = (tableName: string, schema: string = "public") => {
  return `DROP TABLE IF EXISTS ${schema}.${tableName}`;
};

export const alterTable = (
  tableName: string,
  params: { add?: string[]; drop?: string[]; dropConstraints?: string[] },
  schema: string = "public"
): string => {
  const adds = params.add ? params.add.map((add) => `ADD COLUMN ${add}`) : [];
  const drops = params.drop ? params.drop.map((columnName) => `DROP COLUMN ${columnName}`) : [];
  const dropConstraints = params.dropConstraints
    ? params.dropConstraints.map((constraint) => `DROP CONSTRAINT ${constraint}`)
    : [];
  const alterations = [...adds, ...drops, ...dropConstraints];
  return `ALTER TABLE ${schema}.${tableName} ${alterations.join(", ")};`;
};

export const insertIntoTable = (
  tableName: string,
  valuesByColumn: Record<string, unknown>,
  schema: string = "public"
): string => {
  const keys = Object.keys(valuesByColumn);
  const values = keys
    .map((key) => valuesByColumn[key])
    .map((value) => (typeof value === "string" ? `'${value}'` : value));
  return `INSERT INTO ${schema}.${tableName}(${keys.join(", ")}) VALUES(${values.join(", ")});`;
};

export const insertMultipleIntoTable = (
  tableName: string,
  valuesByColumns: Array<Record<string, unknown>>,
  schema: string = "public"
): string => valuesByColumns.map((valuesByColumn) => insertIntoTable(tableName, valuesByColumn, schema)).join("\n");

// ============================================================================
// Predefined Queries for Test Tables
// ============================================================================

// Users table

export const getCreateUsersTableQuery = (tableName: string, schema: string = "public") =>
  createTable(
    tableName,
    [
      "id SERIAL",
      "name VARCHAR(200) NULL",
      "email VARCHAR(200) NULL",
      "updated_at TIMESTAMP",
      `CONSTRAINT ${tableName}_pkey PRIMARY KEY (id)`,
    ],
    schema
  );

export const createUsersTableQuery = getCreateUsersTableQuery("users");
export const insertUsersTableQuery = insertMultipleIntoTable("users", [
  { name: "Abigail", email: "abigail@example.com", updated_at: "2022-12-19 00:00:00" },
  { name: "Andrew", email: "andrew@example.com", updated_at: "2022-12-19 00:00:00" },
  { name: "Kat", email: "kat@example.com", updated_at: "2022-12-19 00:00:00" },
]);

export const getDropUsersTableQuery = (tableName: string, schema: string = "public") => dropTable(tableName, schema);

export const dropUsersTableQuery = getDropUsersTableQuery("users");

// User cars

export const createUserCarsTableQuery = (schema: string = "public") =>
  createTable("user_cars", ["user_id INTEGER", "car_id INTEGER", "created_at TIMESTAMP"], schema);

export const dropUserCarsTableQuery = (schema: string = "public") => dropTable("user_cars", schema);

// Accounts table

export const createAccountsTableQuery = (schema: string = "public") =>
  createTable(
    "accounts",
    ["id SERIAL", "name VARCHAR(200) NULL", "updated_at TIMESTAMP", "CONSTRAINT accounts_pkey PRIMARY KEY (id)"],
    schema
  );

export const dropAccountsTableQuery = (schema: string = "public") => dropTable("accounts", schema);

// Cities table
export const createCitiesTableQuery = (schema: string = "public") =>
  createTable("cities", ["city_code VARCHAR(8)", "city VARCHAR(200)"], schema);

export const insertCitiesTableQuery = (schema: string = "public") =>
  insertMultipleIntoTable(
    "cities",
    [
      {
        city_code: "BCN",
        city: "Barcelona",
      },
      { city_code: "MAD", city: "Madrid" },
      { city_code: "VAL", city: "Valencia" },
    ],
    schema
  );

export const alterCitiesTableQuery = (schema: string = "public") =>
  alterTable(
    "cities",
    {
      add: ["state TEXT", "country TEXT"],
      drop: ["city_code"],
    },
    schema
  );

export const reverseAlterCitiesTableQuery = (schema: string = "public") =>
  alterTable(
    "cities",
    {
      drop: ["state", "country"],
      add: ["city_code VARCHAR(8)"],
    },
    schema
  );

export const dropCitiesTableQuery = (schema: string = "public") => dropTable("cities", schema);

// Alter users table for non-breaking schema changes (add/remove non-critical fields)
export const alterUsersTableQuery = (schema: string = "public") =>
  alterTable(
    "users",
    {
      add: ["phone VARCHAR(20)", "address TEXT"],
      drop: ["email"],
    },
    schema
  );

export const reverseAlterUsersTableQuery = (schema: string = "public") =>
  alterTable(
    "users",
    {
      drop: ["phone", "address"],
      add: ["email VARCHAR(200)"],
    },
    schema
  );

// Cars table
export const createCarsTableQuery = (schema: string = "public") =>
  createTable(
    "cars",
    ["id SERIAL PRIMARY KEY", "mark VARCHAR(200)", "model VARCHAR(200)", "color VARCHAR(200)"],
    schema
  );

export const dropCarsTableQuery = (schema: string = "public") => dropTable("cars", schema);

// Dummy tables - used only for populating stream table with a lot of streams(tables)
// NOTE: Not for testing stream functionality!
export const createDummyTablesQuery = (amountOfTables: number, schema: string = "public") =>
  Array.from({ length: amountOfTables }, (_, index) => {
    const tableName = `dummy_table_${index + 1}`;
    const columns = [
      "id serial PRIMARY KEY",
      "column_1 INTEGER NOT NULL",
      "column_2 VARCHAR(100) NOT NULL",
      "column_3 DECIMAL(10, 2) NOT NULL",
    ];
    return createTable(tableName, columns, schema);
  }).join("\n");

export const dropDummyTablesQuery = (amountOfTables: number, schema: string = "public") => {
  // postgres doesn't allow to drop multiple tables using wildcard, so need to compose the list of table names
  const tables = Array.from({ length: amountOfTables }, (_, index) => `${schema}.dummy_table_${index + 1}`).join(", ");
  return `DROP TABLE IF EXISTS ${tables}`;
};

// Lots of tables

export const createTableWithLotsOfColumnsQuery = (schema: string = "public") => {
  const columns: string[] = [];
  for (let i = 0; i < 50; i++) {
    columns.push(`field_${i} INTEGER NULL`);
  }
  return createTable("columns", columns, schema);
};

export const dropTableWithLotsOfColumnsQuery = (schema: string = "public") => dropTable("columns", schema);

// ============================================================================
// Database Connection and Helpers
// ============================================================================

// Database configuration
// For direct DB access from test code (separate from connector configuration)
// Test code runs on host → uses 127.0.0.1
// Connectors run in Docker → use host.docker.internal
// Both connect to the same Postgres instance from different contexts
const DB_CONFIG = {
  user: process.env.POSTGRES_DIRECT_USER || "postgres",
  host: process.env.POSTGRES_DIRECT_HOST || "127.0.0.1",
  database: process.env.POSTGRES_DIRECT_DB || "airbyte_ci_source",
  password: process.env.POSTGRES_DIRECT_PASSWORD || "secret_password",
  port: Number(process.env.POSTGRES_DIRECT_PORT || 5433),
};

// Singleton connection pool
let db: pgPromise.IDatabase<unknown> | null = null;

/**
 * Gets or creates the database connection pool
 */
const getDb = (): pgPromise.IDatabase<unknown> => {
  if (!db) {
    db = pgp(DB_CONFIG);
  }
  return db;
};

/** Database helper functions for test setup and manipulation */
export const dbHelpers = {
  runQuery: async (query: string): Promise<void> => {
    const database = getDb();
    try {
      await database.any(query);
    } catch (error) {
      console.error(`[Database] Query failed: ${query}`);
      throw error;
    }
  },

  createSchema: async (schema: string): Promise<void> => {
    return dbHelpers.runQuery(`CREATE SCHEMA IF NOT EXISTS ${schema}`);
  },

  dropSchema: async (schema: string): Promise<void> => {
    return dbHelpers.runQuery(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
  },

  /**
   * Resets cities table to initial state (without state/country columns and without PK)
   * This is needed because the setup script creates cities with state/country already present
   * Note: Cities table deliberately has NO primary key to test user-defined PK selection
   */
  resetCitiesToInitialState: async (schema: string = "public"): Promise<void> => {
    await dbHelpers.runQuery(`DROP TABLE IF EXISTS ${schema}.cities CASCADE`);
    await dbHelpers.runQuery(`CREATE TABLE ${schema}.cities (id SERIAL, city VARCHAR(100), city_code VARCHAR(10))`);
    return dbHelpers.runQuery(
      `INSERT INTO ${schema}.cities (city, city_code) VALUES ('New York', 'NYC'), ('Los Angeles', 'LAX'), ('London', 'LON'), ('Tokyo', 'TYO'), ('Sydney', 'SYD')`
    );
  },

  /** Creates users table with baseline test data */
  ensureUsersTableExists: async (schema: string = "public"): Promise<void> => {
    await dbHelpers.runQuery(`DROP TABLE IF EXISTS ${schema}.users CASCADE`);
    await dbHelpers.runQuery(getCreateUsersTableQuery("users", schema));
    return dbHelpers.runQuery(
      insertMultipleIntoTable(
        "users",
        [
          { name: "Abigail", email: "abigail@example.com", updated_at: "2022-12-19 00:00:00" },
          { name: "Andrew", email: "andrew@example.com", updated_at: "2022-12-19 00:00:00" },
          { name: "Kat", email: "kat@example.com", updated_at: "2022-12-19 00:00:00" },
        ],
        schema
      )
    );
  },

  /** Creates cars table with test data */
  ensureCarsTableExists: async (schema: string = "public"): Promise<void> => {
    await dbHelpers.runQuery(dropCarsTableQuery(schema));
    await dbHelpers.runQuery(createCarsTableQuery(schema));
    // Insert test data matching postgres-test-data.sql
    return dbHelpers.runQuery(`
      INSERT INTO ${schema}.cars (mark, model, color) VALUES 
        ('Toyota', 'Camry', 'Silver'),
        ('Honda', 'Civic', 'Blue'),
        ('Ford', 'Focus', 'Red'),
        ('Tesla', 'Model 3', 'White'),
        ('BMW', 'X5', 'Black');
    `);
  },

  /** Creates schema and initializes baseline tables for parallel test execution */
  setupWorkerSchema: async (schema: string): Promise<void> => {
    await dbHelpers.createSchema(schema);
    return dbHelpers.resetToInitialState(schema);
  },

  /** Drops schema and all its tables */
  teardownWorkerSchema: async (schema: string): Promise<void> => {
    return dbHelpers.dropSchema(schema);
  },

  /** Resets to baseline state: users + cities tables (no cars) */
  resetToInitialState: async (schema: string = "public"): Promise<void> => {
    await dbHelpers.ensureUsersTableExists(schema);
    await dbHelpers.resetCitiesToInitialState(schema);
    return dbHelpers.runQuery(dropCarsTableQuery(schema));
  },

  /** Resets to full baseline state: users + cities + cars tables */
  resetToFullBaseline: async (schema: string = "public"): Promise<void> => {
    await dbHelpers.ensureUsersTableExists(schema);
    await dbHelpers.resetCitiesToInitialState(schema);
    return dbHelpers.ensureCarsTableExists(schema);
  },

  /** Simulates schema changes: drops cities, alters users, adds cars */
  makeChangesInDBSource: async (schema: string = "public"): Promise<void> => {
    await dbHelpers.runQuery(dropCitiesTableQuery(schema));
    await dbHelpers.runQuery(alterUsersTableQuery(schema));
    await dbHelpers.runQuery(dropCarsTableQuery(schema)); // Clean up cars first in case it exists
    return dbHelpers.runQuery(createCarsTableQuery(schema));
  },

  /** Reverses schema changes made by makeChangesInDBSource */
  reverseChangesInDBSource: async (schema: string = "public"): Promise<void> => {
    await dbHelpers.resetCitiesToInitialState(schema);
    await dbHelpers.runQuery(reverseAlterUsersTableQuery(schema));
    return dbHelpers.runQuery(dropCarsTableQuery(schema));
  },

  /** Drops all test tables (users, cities, cars) */
  cleanDBSource: async (schema: string = "public"): Promise<void> => {
    await dbHelpers.runQuery(getDropUsersTableQuery("users", schema));
    await dbHelpers.runQuery(dropCitiesTableQuery(schema));
    return dbHelpers.runQuery(dropCarsTableQuery(schema));
  },
};

/** Closes the database connection pool */
export const closeDbConnection = async (): Promise<void> => {
  if (db) {
    try {
      console.log("[Database] Closing connection pool...");
      await db.$pool.end();
      db = null;
      console.log("[Database] Connection pool closed successfully");
    } catch (error) {
      console.error("[Database] Failed to close connection pool:", error);
      // Set to null anyway to prevent reuse attempts
      db = null;
    }
  }
};
