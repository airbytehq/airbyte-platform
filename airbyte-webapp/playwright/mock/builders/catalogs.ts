import type {
  AirbyteCatalog,
  AirbyteStreamAndConfiguration,
  AirbyteStreamConfiguration,
} from "@src/core/api/types/AirbyteClient";

import { createMockPokeApiCatalog, createMockPostgresCatalog } from "../../helpers/mocks";

export interface CatalogOptions {
  /** Sets `selected` on every stream. */
  selected?: boolean;
  /** Per-stream config overrides, keyed by stream name. Applied after `selected`. */
  streams?: Record<string, Partial<AirbyteStreamConfiguration>>;
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

function configure(catalog: AirbyteCatalog, { selected, streams = {} }: CatalogOptions): AirbyteCatalog {
  const unknownStreams = Object.keys(streams).filter(
    (name) => !catalog.streams.some(({ stream }) => stream?.name === name)
  );
  if (unknownStreams.length > 0) {
    throw new Error(`Catalog has no stream named ${unknownStreams.join(", ")}`);
  }

  return {
    streams: catalog.streams.map(({ stream, config }) => ({
      stream,
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
