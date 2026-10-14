/**
 * Mock backend for Playwright tests. Import everything a test needs from this file.
 */
export { scenario } from "./scenario";
export { defs, source, destination } from "./builders/connectors";
export { connection } from "./builders/connection";
export { catalogs } from "./builders/catalogs";
export { ids } from "./ids";

export type { Scenario, ScenarioInput } from "./scenario";
export type { ConnectionHandle, ConnectionInput } from "./builders/connection";
export type { SourceHandle, DestinationHandle } from "./builders/connectors";
export type { CatalogOptions } from "./builders/catalogs";
export type { Store } from "./store";
export type { Op, Body, Reply } from "./routes";
