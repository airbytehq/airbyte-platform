import {
  alterCitiesTableQuery,
  createCarsTableQuery,
  createCitiesTableQuery,
  createUsersTableQuery,
  dropCarsTableQuery,
  dropCitiesTableQuery,
  dropUsersTableQuery,
  insertCitiesTableQuery,
  insertUsersTableQuery,
  reverseAlterCitiesTableQuery,
} from "./queries";

/**
 * Wrapper for DB Query Cypress task
 * @param queryStrings
 */
export const runDbQuery = <T>(...queryStrings: string[]) => cy.task<T>("dbQuery", { query: queryStrings.join("; ") });

export const populateDBSource = () => {
  cleanDBSource();
  runDbQuery(createUsersTableQuery);
  runDbQuery(insertUsersTableQuery);
  runDbQuery(createCitiesTableQuery);
  runDbQuery(insertCitiesTableQuery);
};

export const makeChangesInDBSource = () => {
  runDbQuery(dropUsersTableQuery);
  runDbQuery(alterCitiesTableQuery);
  runDbQuery(createCarsTableQuery);
};

export const reverseChangesInDBSource = () => {
  runDbQuery(createUsersTableQuery);
  runDbQuery(reverseAlterCitiesTableQuery);
  runDbQuery(dropCarsTableQuery);
};

export const cleanDBSource = () => {
  runDbQuery(dropUsersTableQuery);
  runDbQuery(dropCitiesTableQuery);
  runDbQuery(dropCarsTableQuery);
};
