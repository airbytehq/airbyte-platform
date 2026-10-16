/**
 * Self-tests of the mock backend's store (playwright/mock/store.ts). They need no browser page.
 */
import { test, expect } from "@playwright/test";
import destinationIds from "@src/area/connector/utils/destinations.json";
import sourceIds from "@src/area/connector/utils/sources.json";

import { catalogs, connection, defs, destination, scenario, source } from "../../mock";
import { createStore } from "../../mock/store";

const hostSpecification = { type: "object", properties: { host: { type: "string" } } };
const pokeSource = source({ def: defs.pokeApiSource });
const pokeConnection = connection({
  source: pokeSource,
  destination: destination({ def: defs.e2eTestingDestination }),
  catalog: catalogs.pokeApi({ selected: true }),
});

test.describe("Mock backend store", () => {
  test("reads are frozen snapshots that a later write does not change", () => {
    const store = createStore(scenario({ connections: [pokeConnection] }));
    const before = store.connections.get(pokeConnection);

    store.connections.update(pokeConnection, { status: "deprecated" });

    expect(Object.isFrozen(before.syncCatalog.streams[0])).toBe(true);
    expect(before.status).toBe("active");
    expect(store.connections.get(pokeConnection).status).toBe("deprecated");
  });

  test("with() keeps the entity and its ID, copy() makes a new entity with the next ID", () => {
    const renamed = pokeConnection.with({ name: "Renamed" });
    const second = pokeConnection.copy({ name: "Second" });
    const store = createStore(scenario({ connections: [renamed, second] }));

    expect(store.connections.get(pokeConnection)).toMatchObject({
      connectionId: "c0000000-0000-4000-8000-000000000001",
      name: "Renamed",
    });
    expect(store.connections.get(second)).toMatchObject({
      connectionId: "c0000000-0000-4000-8000-000000000002",
      name: "Second",
    });
  });

  test("fails when a scenario uses two with() variants of one entity", () => {
    const renamedSource = pokeSource.with({ name: "Renamed source" });
    const otherConnection = pokeConnection.copy({ source: renamedSource });

    expect(() => createStore(scenario({ connections: [pokeConnection, otherConnection] }))).toThrow(
      `The scenario uses two different with() variants of ${pokeSource.key}. Use one variant per entity.`
    );
  });

  test("fails when a scenario uses a with() variant of a source definition", () => {
    const postgresWithSpec = defs.postgresSource.with({ specification: hostSpecification });
    const first = pokeConnection.copy({ source: source({ def: defs.postgresSource }) });
    const second = pokeConnection.copy({ source: source({ def: postgresWithSpec }) });

    expect(() => createStore(scenario({ connections: [first, second] }))).toThrow(
      `The scenario uses two different versions of the Postgres source definition (${sourceIds.Postgres}). ` +
        "Use one definition handle per connector definition."
    );
  });

  test("fails when a scenario uses a copy() of a source definition", () => {
    const postgresCopy = defs.postgresSource.copy({ specification: hostSpecification });
    const first = pokeConnection.copy({ source: source({ def: defs.postgresSource }) });
    const second = pokeConnection.copy({ source: source({ def: postgresCopy }) });

    expect(() => createStore(scenario({ connections: [first, second] }))).toThrow(
      `The scenario uses two different versions of the Postgres source definition (${sourceIds.Postgres}). ` +
        "Use one definition handle per connector definition."
    );
  });

  test("fails when a scenario uses two versions of a destination definition", () => {
    const postgresWithSpec = defs.postgresDestination.with({ specification: hostSpecification });
    const first = pokeConnection.copy({ destination: destination({ def: defs.postgresDestination }) });
    const second = pokeConnection.copy({ destination: destination({ def: postgresWithSpec }) });

    expect(() => createStore(scenario({ connections: [first, second] }))).toThrow(
      `The scenario uses two different versions of the Postgres destination definition (${destinationIds.Postgres}). ` +
        "Use one definition handle per connector definition."
    );
  });

  test("allows two sources that use the same definition handle", () => {
    const first = pokeConnection.copy({ source: source({ def: defs.postgresSource }) });
    const second = pokeConnection.copy({ source: source({ def: defs.postgresSource }) });
    const store = createStore(scenario({ connections: [first, second] }));

    expect(store.sources.all()).toHaveLength(2);
    expect(store.definitions.allSources()).toHaveLength(1);
  });

  test("gives a new entity an ID that no deleted entity had, and fails on an add with an existing ID", () => {
    const store = createStore(scenario({ sources: [pokeSource, pokeSource.copy()] }));
    const first = store.sources.get(pokeSource);
    const second = store.sources.all()[1];

    store.sources.remove(first.sourceId);
    const newId = store.sources.nextId();

    expect(newId).toBe("50000000-0000-4000-8000-000000000003");
    expect(() => store.sources.add(second.sourceId, second)).toThrow(
      `The mock store already has a source with ID ${second.sourceId}`
    );
  });

  test("fails on a read of an entity that is not in the scenario", () => {
    const store = createStore(scenario({ connections: [pokeConnection] }));

    expect(store.connections.find(pokeConnection.copy())).toBeUndefined();
    expect(() => store.connections.get("no-such-id")).toThrow("The mock store has no connection with ID no-such-id");
  });

  test("instance and user in the scenario change the identity, and the IDs stay the same", () => {
    const store = createStore(
      scenario({ instance: { initialSetupComplete: false }, user: { email: "admin@example.com", name: "Admin" } })
    );
    const defaults = createStore(scenario({})).identity;

    expect(store.identity.workspace.initialSetupComplete).toBe(false);
    expect(store.identity.user).toEqual({ ...defaults.user, email: "admin@example.com", name: "Admin" });
    expect(defaults.workspace.initialSetupComplete).toBe(true);
  });

  test("updateIdentity saves fields, keeps the IDs, and leaves earlier reads unchanged", () => {
    const store = createStore(scenario({}));
    const before = store.identity;

    store.updateIdentity({ workspace: { name: "Renamed", workspaceId: "other-id" } });

    expect(store.identity.workspace).toEqual({ ...before.workspace, name: "Renamed" });
    expect(before.workspace.name).toBe("Test workspace");
    expect(Object.isFrozen(store.identity.workspace)).toBe(true);
  });

  test("catalogs remove the fields that withoutFields names, and fail on a name they do not have", () => {
    const catalog = catalogs.postgres({ withoutFields: { users: ["updated_at"] } });
    const users = catalogs.find(catalog, "public", "users");
    const cars = catalogs.find(catalog, "public", "cars");

    expect(Object.keys(users.stream?.jsonSchema?.properties ?? {})).toEqual(["name", "id", "email"]);
    expect(cars).toEqual(catalogs.find(catalogs.postgres(), "public", "cars"));
    expect(() => catalogs.postgres({ withoutFields: { users: ["no_such_field"] } })).toThrow(
      "Stream users has no field named no_such_field"
    );
    expect(() => catalogs.postgres({ withoutFields: { no_such_stream: ["id"] } })).toThrow(
      "Catalog has no stream named no_such_stream"
    );
  });

  test("catalogs add the fields that withFields names, and fail on a field they already have", () => {
    const phone = { type: "string" };
    const catalog = catalogs.postgres({ withoutFields: { users: ["email"] }, withFields: { users: { phone } } });
    const properties = catalogs.find(catalog, "public", "users").stream?.jsonSchema?.properties ?? {};

    expect(Object.keys(properties)).toEqual(["updated_at", "name", "id", "phone"]);
    expect(properties).toHaveProperty("phone", phone);
    expect(() => catalogs.postgres({ withFields: { users: { email: phone } } })).toThrow(
      "Stream users already has a field named email"
    );
    expect(() => catalogs.postgres({ withFields: { no_such_stream: { phone } } })).toThrow(
      "Catalog has no stream named no_such_stream"
    );
  });

  test("catalogs change the type of a field that is in both withoutFields and withFields", () => {
    const idAsString = { type: "string" };
    const catalog = catalogs.postgres({ withoutFields: { users: ["id"] }, withFields: { users: { id: idAsString } } });

    expect(catalogs.find(catalog, "public", "users").stream?.jsonSchema?.properties).toHaveProperty("id", idAsString);
  });

  test("catalogs leave out the streams that withoutStreams names, and fail on a name they do not have", () => {
    const catalog = catalogs.postgres({ withoutStreams: ["cities"] });

    expect(catalog.streams.map(({ stream }) => stream?.name)).toEqual(["cars", "users"]);
    expect(() => catalogs.postgres({ withoutStreams: ["no_such_stream"] })).toThrow(
      "Catalog has no stream named no_such_stream"
    );
  });

  test("catalogs fail when withoutStreams leaves out a stream that another option changes", () => {
    expect(() =>
      catalogs.postgres({ withoutStreams: ["cars"], streams: { cars: { selected: false } }, withFields: { cars: {} } })
    ).toThrow("withoutStreams leaves out cars, which other options change");
  });
});
