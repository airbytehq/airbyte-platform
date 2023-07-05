export const createTable = (tableName: string, columns: string[]): string =>
  `CREATE TABLE ${tableName}(${columns.join(", ")});`;

export const dropTable = (tableName: string) => `DROP TABLE IF EXISTS ${tableName}`;

export const alterTable = (
  tableName: string,
  params: { add?: string[]; drop?: string[]; dropConstraints?: string[] }
): string => {
  const adds = params.add ? params.add.map((add) => `ADD COLUMN ${add}`) : [];
  const drops = params.drop ? params.drop.map((columnName) => `DROP COLUMN ${columnName}`) : [];
  const dropConstraints = params.dropConstraints
    ? params.dropConstraints.map((constraint) => `DROP CONSTRAINT ${constraint}`)
    : [];
  const alterations = [...adds, ...drops, ...dropConstraints];

  return `ALTER TABLE ${tableName} ${alterations.join(", ")};`;
};

export const insertIntoTable = (tableName: string, valuesByColumn: Record<string, unknown>): string => {
  const keys = Object.keys(valuesByColumn);
  const values = keys
    .map((key) => valuesByColumn[key])
    .map((value) => (typeof value === "string" ? `'${value}'` : value));

  return `INSERT INTO ${tableName}(${keys.join(", ")}) VALUES(${values.join(", ")});`;
};

export const insertMultipleIntoTable = (tableName: string, valuesByColumns: Array<Record<string, unknown>>): string =>
  valuesByColumns.map((valuesByColumn) => insertIntoTable(tableName, valuesByColumn)).join("\n");

// Users table

export const getCreateUsersTableQuery = (tableName: string) =>
  createTable(`public.${tableName}`, [
    "id SERIAL",
    "name VARCHAR(200) NULL",
    "email VARCHAR(200) NULL",
    "updated_at TIMESTAMP",
    `CONSTRAINT ${tableName}_pkey PRIMARY KEY (id)`,
  ]);

export const createUsersTableQuery = getCreateUsersTableQuery("users");
export const insertUsersTableQuery = insertMultipleIntoTable("public.users", [
  { name: "Abigail", email: "abigail@example.com", updated_at: "2022-12-19 00:00:00" },
  { name: "Andrew", email: "andrew@example.com", updated_at: "2022-12-19 00:00:00" },
  { name: "Kat", email: "kat@example.com", updated_at: "2022-12-19 00:00:00" },
]);

export const getDropUsersTableQuery = (tableName: string) => dropTable(`public.${tableName}`);

export const dropUsersTableQuery = getDropUsersTableQuery("users");

// User cars

export const createUserCarsTableQuery = createTable("public.user_cars", [
  "user_id INTEGER",
  "car_id INTEGER",
  "created_at TIMESTAMP",
]);

export const dropUserCarsTableQuery = dropTable("public.user_cars");

// Accounts table

export const createAccountsTableQuery = createTable("public.accounts", [
  "id SERIAL",
  "name VARCHAR(200) NULL",
  "updated_at TIMESTAMP",
  "CONSTRAINT accounts_pkey PRIMARY KEY (id)",
]);

export const dropAccountsTableQuery = dropTable("public.accounts");

// Cities table
export const createCitiesTableQuery = createTable("public.cities", ["city_code VARCHAR(8)", "city VARCHAR(200)"]);

export const insertCitiesTableQuery = insertMultipleIntoTable("public.cities", [
  {
    city_code: "BCN",
    city: "Barcelona",
  },
  { city_code: "MAD", city: "Madrid" },
  { city_code: "VAL", city: "Valencia" },
]);

export const alterCitiesTableQuery = alterTable("public.cities", {
  add: ["state TEXT", "country TEXT"],
  drop: ["city_code"],
});

export const reverseAlterCitiesTableQuery = alterTable("public.cities", {
  drop: ["state", "country"],
  add: ["city_code VARCHAR(8)"],
});
export const dropCitiesTableQuery = dropTable("public.cities");

// Cars table
export const createCarsTableQuery = createTable("public.cars", [
  "id SERIAL PRIMARY KEY",
  "mark VARCHAR(200)",
  "model VARCHAR(200)",
  "color VARCHAR(200)",
]);

export const dropCarsTableQuery = dropTable("public.cars");

// Dummy tables - used only for populating stream table with a lot of streams(tables)
// NOTE: Not for testing stream functionality!
export const createDummyTablesQuery = (amountOfTables: number) =>
  Array.from({ length: amountOfTables }, (_, index) => {
    const tableName = `public.dummy_table_${index + 1}`;
    const columns = [
      "id serial PRIMARY KEY",
      "column_1 INTEGER NOT NULL",
      "column_2 VARCHAR(100) NOT NULL",
      "column_3 DECIMAL(10, 2) NOT NULL",
    ];
    return createTable(tableName, columns);
  }).join("\n");

export const dropDummyTablesQuery = (amountOfTables: number) => {
  // postgres doesn't allow to drop multiple tables using wildcard, so need to compose the list of table names
  const tables = Array.from({ length: amountOfTables }, (_, index) => `public.dummy_table_${index + 1}`).join(", ");
  return dropTable(tables);
};

// Lots of tables

export const createTableWithLotsOfColumnsQuery = (() => {
  const columns: string[] = [];
  for (let i = 0; i < 50; i++) {
    columns.push(`field_${i} INTEGER NULL`);
  }
  return createTable("public.columns", columns);
})();

export const dropTableWithLotsOfColumnsQuery = dropTable("public.columns");
