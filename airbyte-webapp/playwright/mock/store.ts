import type { BuilderProjects } from "./builderProjects";
import type { ConnectionHandle } from "./builders/connection";
import type {
  DestinationDefinitionHandle,
  DestinationDefinitionInput,
  DestinationHandle,
  SourceDefinitionHandle,
  SourceDefinitionInput,
  SourceHandle,
} from "./builders/connectors";
import type { Handle } from "./builders/handle";
import type { Commands } from "./commands";
import type { Jobs } from "./jobs";
import type { Scenario } from "./scenario";
import type {
  ActorDefinitionVersionRead,
  AirbyteCatalog,
  CatalogDiff,
  DestinationRead,
  OrganizationRead,
  SchemaChange,
  SourceRead,
  UserRead,
  WebBackendConnectionRead,
  WorkspaceRead,
} from "@src/core/api/types/AirbyteClient";

import { createBuilderProjects } from "./builderProjects";
import { MOCK_START_TIME } from "./clock";
import { createCommands } from "./commands";
import { ids } from "./ids";
import { createJobs } from "./jobs";

/**
 * One kind of entity in the mock store, such as connections. A read returns a copy that cannot be changed.
 * To see a change, read again.
 */
export interface Collection<Entity, Stored, H> {
  /** Throws when the entity does not exist. */
  get(ref: H | string): Readonly<Entity>;
  find(ref: H | string): Readonly<Entity> | undefined;
  all(): ReadonlyArray<Readonly<Entity>>;
  /**
   * Returns a new ID for the next entity. The number in the ID only goes up, so an entity that the test deleted
   * never gives its ID to a new entity.
   */
  nextId(): string;
  /** Adds an entity that the webapp created during the test. Throws when an entity with this ID exists. */
  add(id: string, stored: Stored): Readonly<Entity>;
  update(ref: H | string, patch: Partial<Stored>): Readonly<Entity>;
  /** Removes an entity that the webapp deleted. Reads and lists no longer find it. Throws when it does not exist. */
  remove(ref: H | string): void;
}

/** Stored connection fields. The source, destination and actor definition versions are derived on each read. */
export type StoredConnection = Omit<
  WebBackendConnectionRead,
  "source" | "destination" | "sourceActorDefinitionVersion" | "destinationActorDefinitionVersion"
>;

export interface Identity {
  user: UserRead;
  workspace: WorkspaceRead;
  organization: OrganizationRead;
}

/** What a schema refresh of a connection finds. The test supplies it, because the mock computes no diff. */
export interface SchemaRefreshAnswer {
  /** The refreshed sync catalog, with the stream config the server keeps from the current catalog. */
  catalog: AirbyteCatalog;
  diff: CatalogDiff;
  schemaChange: SchemaChange;
}

export interface Store {
  readonly identity: Readonly<Identity>;
  /** Saves fields of the user, the workspace or the organization. The IDs do not change. */
  updateIdentity(patch: { [K in keyof Identity]?: Partial<Identity[K]> }): Readonly<Identity>;
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
  /** The answer to the next schema refreshes of each connection. `airbyte.schema.onRefresh` sets it. */
  readonly schemaRefreshes: {
    set(ref: ConnectionHandle | string, answer: SchemaRefreshAnswer): void;
    find(ref: ConnectionHandle | string): Readonly<SchemaRefreshAnswer> | undefined;
  };
  /** Connection checks that the webapp started. */
  readonly commands: Commands;
  /** Sync jobs that the webapp started, and their timeline events. */
  readonly jobs: Jobs;
  /** Connector Builder projects that the webapp created. */
  readonly builderProjects: BuilderProjects;
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
  private readonly makeId: (n: number) => string;
  private readonly toEntity: (stored: Stored) => Entity;
  /** How many IDs nextId() has given. A delete does not lower it. */
  private issuedIds = 0;

  constructor(label: string, makeId: (n: number) => string, toEntity: (stored: Stored) => Entity) {
    this.label = label;
    this.makeId = makeId;
    this.toEntity = toEntity;
  }

  nextId() {
    this.issuedIds += 1;
    return this.makeId(this.issuedIds);
  }

  /** Adds the entity that a scenario handle describes. */
  addFromHandle(handle: H, id: string, stored: Stored) {
    this.assertNewId(id);
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

  add(id: string, stored: Stored) {
    this.assertNewId(id);
    this.records.set(id, structuredClone(stored));
    return this.get(id);
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

  remove(ref: H | string) {
    const id = this.idOf(ref);
    if (!id || !this.records.delete(id)) {
      throw new Error(`The mock store has no ${this.label} ${this.describe(ref)}`);
    }
  }

  /** A second entity with the same ID would silently replace the first one, so fail the test instead. */
  private assertNewId(id: string) {
    if (this.records.has(id)) {
      throw new Error(`The mock store already has a ${this.label} with ID ${id}`);
    }
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

function createIdentity(scenario: Scenario): Identity {
  const organizationId = ids.organization(1);
  const user: UserRead = {
    email: "test@example.com",
    name: "Test User",
    metadata: {},
    ...scenario.input.user,
    userId: ids.user(1),
  };
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
    // The server reports this flag of the default workspace as the initialSetupComplete of the instance.
    initialSetupComplete: scenario.input.instance?.initialSetupComplete ?? true,
    displaySetupWizard: false,
    anonymousDataCollection: false,
  };
  return { user, workspace, organization };
}

/**
 * Builds a fresh store from a scenario. IDs follow the order in which the scenario lists the entities.
 * `nowSeconds` gives the time for the timestamps the store writes. The mock backend passes the browser time.
 */
export function createStore(
  scenario: Scenario,
  nowSeconds = () => Math.floor(MOCK_START_TIME.getTime() / 1000)
): Store {
  let identity = snapshot(createIdentity(scenario));
  const sourceDefinitions = new Map<string, SourceDefinitionInput>();
  const destinationDefinitions = new Map<string, DestinationDefinitionInput>();

  const sources = new EntityCollection<SourceRead, SourceRead, SourceHandle>("source", ids.source, (stored) => stored);
  const destinations = new EntityCollection<DestinationRead, DestinationRead, DestinationHandle>(
    "destination",
    ids.destination,
    (stored) => stored
  );
  const connections = new EntityCollection<WebBackendConnectionRead, StoredConnection, ConnectionHandle>(
    "connection",
    ids.connection,
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

  function addSourceDefinition(def: SourceDefinitionHandle) {
    assertOneDefinition("source", def.input.definition.sourceDefinitionId, def);
    sourceDefinitions.set(def.input.definition.sourceDefinitionId, structuredClone(def.input));
  }

  function addSource(handle: SourceHandle) {
    assertOneVariant(handle);
    const existingId = sources.idOf(handle);
    if (existingId) {
      return existingId;
    }

    const { def, name, config } = handle.input;
    addSourceDefinition(def);
    const id = sources.nextId();
    sources.addFromHandle(handle, id, {
      sourceId: id,
      sourceDefinitionId: def.input.definition.sourceDefinitionId,
      workspaceId: identity.workspace.workspaceId,
      name: name ?? def.input.definition.name,
      sourceName: def.input.definition.name,
      connectionConfiguration: structuredClone(config ?? {}),
      createdAt: 0,
      isDraft: false,
      // OSS entitles every connector (SourceHandler.kt). The source page disables its controls without it.
      isEntitled: true,
    });
    return id;
  }

  function addDestinationDefinition(def: DestinationDefinitionHandle) {
    assertOneDefinition("destination", def.input.definition.destinationDefinitionId, def);
    destinationDefinitions.set(def.input.definition.destinationDefinitionId, structuredClone(def.input));
  }

  function addDestination(handle: DestinationHandle) {
    assertOneVariant(handle);
    const existingId = destinations.idOf(handle);
    if (existingId) {
      return existingId;
    }

    const { def, name, config } = handle.input;
    addDestinationDefinition(def);
    const id = destinations.nextId();
    destinations.addFromHandle(handle, id, {
      destinationId: id,
      destinationDefinitionId: def.input.definition.destinationDefinitionId,
      workspaceId: identity.workspace.workspaceId,
      name: name ?? def.input.definition.name,
      destinationName: def.input.definition.name,
      connectionConfiguration: structuredClone(config ?? {}),
      createdAt: 0,
      isDraft: false,
      // OSS entitles every connector (DestinationHandler.kt). The destination page disables its controls without it.
      isEntitled: true,
    });
    return id;
  }

  (scenario.input.sourceDefinitions ?? []).forEach(addSourceDefinition);
  (scenario.input.sources ?? []).forEach(addSource);
  (scenario.input.destinationDefinitions ?? []).forEach(addDestinationDefinition);
  (scenario.input.destinations ?? []).forEach(addDestination);

  (scenario.input.connections ?? []).forEach((handle, index) => {
    if (connections.idOf(handle)) {
      throw new Error(`The scenario lists ${handle.key} twice. Use copy() for a second connection.`);
    }
    const { source, destination, catalog, ...fields } = handle.input;
    const sourceId = addSource(source);
    const destinationId = addDestination(destination);
    const sourceName = sources.get(sourceId).name;
    const destinationName = destinations.get(destinationId).name;

    const connectionId = connections.nextId();
    connections.addFromHandle(handle, connectionId, {
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
      connectionId,
      catalogId: ids.catalog(index + 1),
      sourceId,
      destinationId,
      syncCatalog: structuredClone(catalog),
    });
  });

  const refreshAnswers = new Map<string, SchemaRefreshAnswer>();
  function connectionIdOf(ref: ConnectionHandle | string) {
    return typeof ref === "string" ? ref : connections.get(ref).connectionId;
  }

  return {
    get identity() {
      return identity;
    },
    updateIdentity: (patch) => {
      identity = snapshot({
        user: { ...identity.user, ...patch.user, userId: identity.user.userId },
        workspace: { ...identity.workspace, ...patch.workspace, workspaceId: identity.workspace.workspaceId },
        organization: {
          ...identity.organization,
          ...patch.organization,
          organizationId: identity.organization.organizationId,
        },
      });
      return identity;
    },
    sources,
    destinations,
    connections,
    definitions: {
      source: (id) => (sourceDefinitions.has(id) ? snapshot(sourceDefinitions.get(id)!) : undefined),
      destination: (id) => (destinationDefinitions.has(id) ? snapshot(destinationDefinitions.get(id)!) : undefined),
      allSources: () => snapshot([...sourceDefinitions.values()]),
      allDestinations: () => snapshot([...destinationDefinitions.values()]),
    },
    schemaRefreshes: {
      set: (ref, answer) => {
        refreshAnswers.set(connectionIdOf(ref), structuredClone(answer));
      },
      find: (ref) => {
        const answer = refreshAnswers.get(connectionIdOf(ref));
        return answer && snapshot(answer);
      },
    },
    commands: createCommands(scenario.input.outcomes?.check),
    jobs: createJobs({
      nowSeconds,
      user: () => ({
        id: identity.user.userId,
        email: identity.user.email,
        name: identity.user.name,
        isDeleted: false,
      }),
      outcome: scenario.input.outcomes?.sync,
    }),
    builderProjects: createBuilderProjects(nowSeconds, scenario.input.outcomes?.streamRead),
  };
}
