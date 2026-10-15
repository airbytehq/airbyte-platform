import type {
  AirbyteCatalog,
  AirbyteStream,
  AirbyteStreamAndConfiguration,
  AirbyteStreamConfiguration,
} from "@src/core/api/types/AirbyteClient";

import { createMockPokeApiCatalog, createMockPostgresCatalog } from "../../helpers/mocks";

export interface CatalogOptions {
  /** Sets `selected` on every stream. */
  selected?: boolean;
  /** Per-stream config overrides, keyed by stream name. Applied after `selected`. */
  streams?: Record<string, Partial<AirbyteStreamConfiguration>>;
  /** Fields to remove from the JSON schema of a stream, keyed by stream name. For example, a column the source dropped. */
  withoutFields?: Record<string, string[]>;
  /**
   * Fields to add to the JSON schema of a stream, keyed by stream name and then by field name. The value is the JSON
   * schema of the field. For example, a column the source added. Applied after `withoutFields`, so a field in both
   * gets a new schema, as when the source changes the type of a column.
   */
  withFields?: Record<string, Record<string, object>>;
  /** Streams to leave out of the catalog. For example, a table the source dropped. */
  withoutStreams?: string[];
}

/**
 * Stream config for streams that have none in the source catalog.
 * These are the values the live tests start from.
 */
const defaultConfig: AirbyteStreamConfiguration = {
  syncMode: "full_refresh",
  destinationSyncMode: "overwrite",
  selected: false,
};

/** Removes fields from the top-level properties of a stream's JSON schema. */
function removeFields(stream: AirbyteStream, fields: string[]): AirbyteStream {
  const properties = (stream.jsonSchema?.properties ?? {}) as Record<string, unknown>;
  const unknownFields = fields.filter((field) => !(field in properties));
  if (unknownFields.length > 0) {
    throw new Error(`Stream ${stream.name} has no field named ${unknownFields.join(", ")}`);
  }
  const kept = Object.fromEntries(Object.entries(properties).filter(([field]) => !fields.includes(field)));
  return { ...stream, jsonSchema: { ...stream.jsonSchema, properties: kept } };
}

/** Adds fields to the top-level properties of a stream's JSON schema. */
function addFields(stream: AirbyteStream, fields: Record<string, object>): AirbyteStream {
  const properties = (stream.jsonSchema?.properties ?? {}) as Record<string, unknown>;
  const existingFields = Object.keys(fields).filter((field) => field in properties);
  if (existingFields.length > 0) {
    throw new Error(`Stream ${stream.name} already has a field named ${existingFields.join(", ")}`);
  }
  return { ...stream, jsonSchema: { ...stream.jsonSchema, properties: { ...properties, ...fields } } };
}

/** Removes, then adds, fields of one stream. A stream without changes keeps its original JSON schema. */
function changeFields(
  stream: AirbyteStream,
  fieldsToRemove: string[] | undefined,
  fieldsToAdd: Record<string, object> | undefined
): AirbyteStream {
  const kept = fieldsToRemove ? removeFields(stream, fieldsToRemove) : stream;
  return fieldsToAdd ? addFields(kept, fieldsToAdd) : kept;
}

function configure(
  catalog: AirbyteCatalog,
  { selected, streams = {}, withoutFields = {}, withFields = {}, withoutStreams = [] }: CatalogOptions
): AirbyteCatalog {
  const changedStreams = [...Object.keys(streams), ...Object.keys(withoutFields), ...Object.keys(withFields)];
  const unknownStreams = [...changedStreams, ...withoutStreams].filter(
    (name) => !catalog.streams.some(({ stream }) => stream?.name === name)
  );
  if (unknownStreams.length > 0) {
    throw new Error(`Catalog has no stream named ${unknownStreams.join(", ")}`);
  }
  const changedAndLeftOut = [...new Set(changedStreams)].filter((name) => withoutStreams.includes(name));
  if (changedAndLeftOut.length > 0) {
    throw new Error(`withoutStreams leaves out ${changedAndLeftOut.join(", ")}, which other options change`);
  }

  const keptStreams = catalog.streams.filter(({ stream }) => !withoutStreams.includes(stream?.name ?? ""));
  return {
    streams: keptStreams.map(({ stream, config }) => ({
      stream: stream && changeFields(stream, withoutFields[stream.name], withFields[stream.name]),
      config: {
        ...(config ?? defaultConfig),
        ...(selected === undefined ? {} : { selected }),
        ...streams[stream?.name ?? ""],
      },
    })),
  };
}

/**
 * Catalogs as source discovery returns them. Each call returns a new object.
 * The catalog data comes from helpers/mocks.ts, which the live tests also use for mocked discovery.
 */
export const catalogs = {
  /** public.cars, public.cities, public.users. Every stream starts as full_refresh | overwrite. */
  postgres: (options: CatalogOptions = {}) => configure(createMockPostgresCatalog(), options),
  /** pokemon (no namespace). */
  pokeApi: (options: CatalogOptions = {}) => configure(createMockPokeApiCatalog(), options),

  /** Finds one stream in a catalog, for example in a request body. Throws with the stream names when it is missing. */
  find(
    catalog: AirbyteCatalog | undefined,
    namespace: string | undefined,
    name: string
  ): AirbyteStreamAndConfiguration {
    const streams = catalog?.streams ?? [];
    const found = streams.find(({ stream }) => stream?.namespace === namespace && stream?.name === name);
    if (!found) {
      const names = streams.map(({ stream }) => [stream?.namespace, stream?.name].filter(Boolean).join("."));
      throw new Error(
        `No stream ${[namespace, name].filter(Boolean).join(".")} in catalog. Streams: ${names.join(", ")}`
      );
    }
    return found;
  },
};
