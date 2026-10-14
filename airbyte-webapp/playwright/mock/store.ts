import type { ConnectionHandle } from "./builders/connection";
import type {
  DestinationDefinitionInput,
  DestinationHandle,
  SourceDefinitionInput,
  SourceHandle,
} from "./builders/connectors";
import type { Handle } from "./builders/handle";
import type { Scenario } from "./scenario";
import type {
  ActorDefinitionVersionRead,
  DestinationRead,
  OrganizationRead,
  SourceRead,
  UserRead,
  WebBackendConnectionRead,
  WorkspaceRead,
} from "@src/core/api/types/AirbyteClient";

import { ids } from "./ids";

/**
 * One kind of entity in the mock store, such as connections. A read returns a copy that cannot be changed.
 * To see a change, read again.
 */
export interface Collection<Entity, Stored, H> {
  /** Throws when the entity does not exist. */
  get(ref: H | string): Readonly<Entity>;
  find(ref: H | string): Readonly<Entity> | undefined;
  all(): ReadonlyArray<Readonly<Entity>>;
  update(ref: H | string, patch: Partial<Stored>): Readonly<Entity>;
}

/** Stored connection fields. The source, destination and actor definition versions are derived on each read. */
export type StoredConnection = Omit<
  WebBackendConnectionRead,
  "source" | "destination" | "sourceActorDefinitionVersion" | "destinationActorDefinitionVersion"
>;

export interface Store {
  readonly identity: Readonly<{ user: UserRead; workspace: WorkspaceRead; organization: OrganizationRead }>;
  readonly sources: Collection<SourceRead, SourceRead, SourceHandle>;
  readonly destinations: Collection<DestinationRead, DestinationRead, DestinationHandle>;
  /** Full WebBackendConnectionRead, derived from the stored connection, its source, and its destination. */
  readonly connections: Collection<WebBackendConnectionRead, StoredConnection, ConnectionHandle>;
  /** Definitions of the scenario's sources and destinations. */
  readonly definitions: {
    source(sourceDefinitionId: string): Readonly<SourceDefinitionInput> | undefined;
    destination(destinationDefinitionId: string): Readonly<DestinationDefinitionInput> | undefined;
    allSources(): ReadonlyArray<Readonly<SourceDefinitionInput>>;
    allDestinations(): ReadonlyArray<Readonly<DestinationDefinitionInput>>;
  };
}

export function snapshot<T>(value: T): Readonly<T> {
  return deepFreeze(structuredClone(value));
}

function deepFreeze<T>(value: T): T {
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    Object.freeze(value);
    Object.values(value).forEach(deepFreeze);
  }
  return value;
}

class EntityCollection<Entity, Stored, H extends Handle<string, object>> implements Collection<Entity, Stored, H> {
  private readonly records = new Map<string, Stored>();
  private readonly idsByKey = new Map<string, string>();
  private readonly label: string;
  private readonly toEntity: (stored: Stored) => Entity;

  constructor(label: string, toEntity: (stored: Stored) => Entity) {
    this.label = label;
    this.toEntity = toEntity;
  }

  get size() {
    return this.records.size;
  }

  /** Adds the entity that a scenario handle describes. */
  addFromHandle(handle: H, id: string, stored: Stored) {
    this.idsByKey.set(handle.key, id);
    this.records.set(id, stored);
  }

  idOf(ref: H | string): string | undefined {
    return typeof ref === "string" ? ref : this.idsByKey.get(ref.key);
  }

  find(ref: H | string) {
    const stored = this.findStored(ref);
    return stored && snapshot(this.toEntity(stored));
  }

  get(ref: H | string) {
    const entity = this.find(ref);
    if (!entity) {
      throw new Error(`The mock store has no ${this.label} ${this.describe(ref)}`);
    }
    return entity;
  }

  all() {
    return [...this.records.values()].map((stored) => snapshot(this.toEntity(stored)));
  }

  update(ref: H | string, patch: Partial<Stored>) {
    const id = this.idOf(ref);
    const stored = this.findStored(ref);
    if (!id || !stored) {
      throw new Error(`The mock store has no ${this.label} ${this.describe(ref)}`);
    }
    this.records.set(id, { ...stored, ...structuredClone(patch) });
    return this.get(id);
  }

  private findStored(ref: H | string) {
    const id = this.idOf(ref);
    return id === undefined ? undefined : this.records.get(id);
  }

  private describe(ref: H | string) {
    return typeof ref === "string" ? `with ID ${ref}` : `for handle ${ref.key} (is it in the scenario?)`;
  }
}

/** The version the server reports for an actor. Every definition runs its default version in the mock. */
export function actorDefinitionVersion(definition: { dockerRepository: string; dockerImageTag: string }) {
  return {
    dockerRepository: definition.dockerRepository,
    dockerImageTag: definition.dockerImageTag,
    supportsRefreshes: true,
    isVersionOverrideApplied: false,
    supportState: "supported",
    supportsFileTransfer: false,
    supportsDataActivation: false,
  } satisfies ActorDefinitionVersionRead;
}

function createIdentity() {
  const organizationId = ids.organization(1);
  const user: UserRead = { userId: ids.user(1), email: "test@example.com", name: "Test User", metadata: {} };
  const organization: OrganizationRead = {
    organizationId,
    organizationName: "Test organization",
    email: user.email,
  };
  const workspace: WorkspaceRead = {
    workspaceId: ids.workspace(1),
    customerId: ids.workspace(1),
    organizationId,
    email: user.email,
    name: "Test workspace",
    slug: "test-workspace",
    initialSetupComplete: true,
    displaySetupWizard: false,
    anonymousDataCollection: false,
  };
  return { user, workspace, organization };
}

/** Builds a fresh store from a scenario. IDs follow the order in which the scenario lists the entities. */
export function createStore(scenario: Scenario): Store {
  const identity = snapshot(createIdentity());
  const sourceDefinitions = new Map<string, SourceDefinitionInput>();
  const destinationDefinitions = new Map<string, DestinationDefinitionInput>();

  const sources = new EntityCollection<SourceRead, SourceRead, SourceHandle>("source", (stored) => stored);
  const destinations = new EntityCollection<DestinationRead, DestinationRead, DestinationHandle>(
    "destination",
    (stored) => stored
  );
  const connections = new EntityCollection<WebBackendConnectionRead, StoredConnection, ConnectionHandle>(
    "connection",
    (stored) => {
      const source = sources.get(stored.sourceId);
      const destination = destinations.get(stored.destinationId);
      return {
        ...stored,
        source,
        destination,
        sourceActorDefinitionVersion: actorDefinitionVersion(
          sourceDefinitions.get(source.sourceDefinitionId)!.definition
        ),
        destinationActorDefinitionVersion: actorDefinitionVersion(
          destinationDefinitions.get(destination.destinationDefinitionId)!.definition
        ),
      };
    }
  );

  const seenInputs = new Map<string, object>();
  function assertOneVariant(handle: Handle<string, object>) {
    const seen = seenInputs.get(handle.key);
    if (seen && seen !== handle.input) {
      throw new Error(`The scenario uses two different with() variants of ${handle.key}. Use one variant per entity.`);
    }
    seenInputs.set(handle.key, handle.input);
  }

  const seenDefinitionInputs = new Map<string, object>();
  function assertOneDefinition(
    kind: "source" | "destination",
    id: string,
    def: Handle<string, { definition: { name: string } }>
  ) {
    const seen = seenDefinitionInputs.get(`${kind}:${id}`);
    if (seen && seen !== def.input) {
      throw new Error(
        `The scenario uses two different versions of the ${def.input.definition.name} ${kind} definition (${id}). ` +
          "Use one definition handle per connector definition."
      );
    }
    seenDefinitionInputs.set(`${kind}:${id}`, def.input);
  }

  function addSource(handle: SourceHandle) {
    assertOneVariant(handle);
    const existingId = sources.idOf(handle);
    if (existingId) {
      return existingId;
    }

    const { def, name, config } = handle.input;
    assertOneDefinition("source", def.input.definition.sourceDefinitionId, def);
    const id = ids.source(sources.size + 1);
    sourceDefinitions.set(def.input.definition.sourceDefinitionId, structuredClone(def.input));
    sources.addFromHandle(handle, id, {
      sourceId: id,
      sourceDefinitionId: def.input.definition.sourceDefinitionId,
      workspaceId: identity.workspace.workspaceId,
      name: name ?? def.input.definition.name,
      sourceName: def.input.definition.name,
      connectionConfiguration: structuredClone(config ?? {}),
      createdAt: 0,
      isDraft: false,
    });
    return id;
  }

  function addDestination(handle: DestinationHandle) {
    assertOneVariant(handle);
    const existingId = destinations.idOf(handle);
    if (existingId) {
      return existingId;
    }

    const { def, name, config } = handle.input;
    assertOneDefinition("destination", def.input.definition.destinationDefinitionId, def);
    const id = ids.destination(destinations.size + 1);
    destinationDefinitions.set(def.input.definition.destinationDefinitionId, structuredClone(def.input));
    destinations.addFromHandle(handle, id, {
      destinationId: id,
      destinationDefinitionId: def.input.definition.destinationDefinitionId,
      workspaceId: identity.workspace.workspaceId,
      name: name ?? def.input.definition.name,
      destinationName: def.input.definition.name,
      connectionConfiguration: structuredClone(config ?? {}),
      createdAt: 0,
      isDraft: false,
    });
    return id;
  }

  (scenario.input.connections ?? []).forEach((handle, index) => {
    if (connections.idOf(handle)) {
      throw new Error(`The scenario lists ${handle.key} twice. Use copy() for a second connection.`);
    }
    const { source, destination, catalog, ...fields } = handle.input;
    const sourceId = addSource(source);
    const destinationId = addDestination(destination);
    const sourceName = sources.get(sourceId).name;
    const destinationName = destinations.get(destinationId).name;

    connections.addFromHandle(handle, ids.connection(index + 1), {
      name: `${sourceName} → ${destinationName}`,
      scheduleType: "manual",
      status: "active",
      isSyncing: false,
      schemaChange: "no_change",
      notifySchemaChanges: true,
      notifySchemaChangesByEmail: false,
      nonBreakingChangesPreference: "ignore",
      tags: [],
      onDemandEnabled: false,
      ...structuredClone(fields),
      connectionId: ids.connection(index + 1),
      catalogId: ids.catalog(index + 1),
      sourceId,
      destinationId,
      syncCatalog: structuredClone(catalog),
    });
  });

  return {
    identity,
    sources,
    destinations,
    connections,
    definitions: {
      source: (id) => (sourceDefinitions.has(id) ? snapshot(sourceDefinitions.get(id)!) : undefined),
      destination: (id) => (destinationDefinitions.has(id) ? snapshot(destinationDefinitions.get(id)!) : undefined),
      allSources: () => snapshot([...sourceDefinitions.values()]),
      allDestinations: () => snapshot([...destinationDefinitions.values()]),
    },
  };
}
