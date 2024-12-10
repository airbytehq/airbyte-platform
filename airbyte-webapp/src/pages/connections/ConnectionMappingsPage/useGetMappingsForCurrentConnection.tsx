import { v4 as uuidv4 } from "uuid";

import { useCurrentConnection } from "core/api";
import { ConfiguredStreamMapper } from "core/api/types/AirbyteClient";

export const useGetMappingsForCurrentConnection = (): Record<string, ConfiguredStreamMapper[]> => {
  const connection = useCurrentConnection();

  const mappings: Record<string, ConfiguredStreamMapper[]> = {};

  connection.syncCatalog?.streams.forEach((streamItem) => {
    if (streamItem.config?.mappers && streamItem.config.mappers.length > 0) {
      mappings[streamItem.stream?.name ?? ""] = streamItem.config.mappers.map((mapper) => ({
        ...mapper,
        mapperConfiguration: {
          ...mapper.mapperConfiguration,
          id: uuidv4(),
        },
      }));
    }
  });

  return mappings;
};
