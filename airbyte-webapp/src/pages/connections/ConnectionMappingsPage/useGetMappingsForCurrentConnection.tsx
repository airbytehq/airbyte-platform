import { v4 as uuidv4 } from "uuid";

import { useCurrentConnection } from "core/api";

import { getKeyForStream } from "./MappingContext";
import { StreamMapperWithId } from "./types";

export const useGetMappingsForCurrentConnection = (): Record<string, StreamMapperWithId[]> => {
  const connection = useCurrentConnection();

  const mappings: Record<string, StreamMapperWithId[]> = {};

  connection.syncCatalog?.streams.forEach((streamItem) => {
    if (streamItem.stream && streamItem.config?.mappers && streamItem.config.mappers.length > 0) {
      const key = getKeyForStream(streamItem.stream);

      mappings[key] = streamItem.config.mappers.map((mapper) => ({
        ...mapper,
        id: mapper.id ?? uuidv4(),
        // Mappers returned by the backend are valid by default
        validationCallback: () => Promise.resolve(true),
        mapperConfiguration: {
          ...mapper.mapperConfiguration,
        },
      }));
    }
  });

  return mappings;
};
