/**
 * Mock backend for Playwright tests. Import everything a test needs from this file. For example:
 *
 *   import { catalogs, connection, defs, destination, scenario, source, test, expect } from "../../mock";
 *
 * The rules for tests are in fixture.ts. The rules for handlers and overrides are in handlers/handler.ts.
 */
export { test, expect } from "./fixture";
export { scenario } from "./scenario";
export { defs, source, destination } from "./builders/connectors";
export { connection } from "./builders/connection";
export { catalogs } from "./builders/catalogs";
export { errors } from "./errors";
export { mapperValidation } from "./handlers/mappers";
export { ids } from "./ids";

export type { MockAirbyte, ConnectionTab } from "./backend";
export type { Scenario, ScenarioInput } from "./scenario";
export type { ConnectionHandle, ConnectionInput } from "./builders/connection";
export type { SourceHandle, DestinationHandle } from "./builders/connectors";
export type { CatalogOptions } from "./builders/catalogs";
export type { SchemaRefreshAnswer, Store } from "./store";
export type { SecurityCheckAnswer } from "./external";
export type { Override } from "./handlers/handler";
export type { Recorder } from "./recorder";
export type { Op, Body, Reply } from "./routes";
