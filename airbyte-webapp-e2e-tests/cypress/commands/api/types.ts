export interface Connection {
  connectionId: string;
  destination: Destination;
  destinationId: string;
  isSyncing: boolean;
  name: string;
  scheduleType: string;
  schemaChange: string;
  source: Source;
  sourceId: string;
  status: "active" | "inactive" | "deprecated";
  nonBreakingChangesPreference: "ignore" | "disable";
  syncCatalog: SyncCatalog;
}

export interface ConnectionCreateRequestBody {
  destinationId: string;
  geography: string;
  name: string;
  namespaceDefinition: string;
  namespaceFormat: string;
  nonBreakingChangesPreference: "ignore" | "disable";
  operations: unknown[];
  prefix: string;
  scheduleType: string;
  sourceCatalogId: string;
  sourceId: string;
  status: "active";
  syncCatalog: SyncCatalog;
}

export interface ConnectionGetBody {
  connectionId: string;
  withRefreshedCatalog?: boolean;
}

export interface ConnectionsList {
  connections: Connection[];
}

export interface Destination {
  name: string;
  destinationDefinitionId: string;
  destinationName: string;
  destinationId: string;
  connectionConfiguration: Record<string, unknown>;
}

export interface DestinationsList {
  destinations: Destination[];
}

export interface Source {
  name: string;
  sourceDefinitionId: string;
  sourceName: string;
  sourceId: string;
  connectionConfiguration: Record<string, unknown>;
}

export interface SourceDiscoverSchema {
  catalog: SyncCatalog;
  catalogId: string;
}

export interface SourcesList {
  sources: Source[];
}

export interface SyncCatalog {
  streams: SyncCatalogStreamAndConfig[];
}

export interface SyncCatalogStream {
  name: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  jsonSchema?: any;
  supportedSyncModes?: "full_refresh" | "incremental";
  sourceDefinedCursor?: boolean;
  defaultCursorField?: string[];
  sourceDefinedPrimaryKey?: string[][];
  namespace?: string;
}

export const enum SourceSyncMode {
  FullRefresh = "full_refresh",
  Incremental = "incremental",
}

export const enum DestinationSyncMode {
  Append = "append",
  AppendDedup = "append_dedup",
  Overwrite = "overwrite",
}

export interface SyncCatalogStreamConfig {
  syncMode: SourceSyncMode;
  cursorField?: string[];
  destinationSyncMode: DestinationSyncMode;
  primaryKey?: string[][];
  aliasName?: string;
  selected?: boolean;
  suggested?: boolean;
  fieldSelectionEnabled?: boolean;
  selectedFields?: Array<{ fieldPath?: string }>;
}

export interface SyncCatalogStreamAndConfig {
  config: SyncCatalogStreamConfig;
  stream: SyncCatalogStream;
}
