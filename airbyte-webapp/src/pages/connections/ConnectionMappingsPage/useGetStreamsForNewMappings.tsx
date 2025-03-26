import { useCurrentConnection } from "core/api";

import { getKeyForStream, useMappingContext } from "./MappingContext";

export const useGetStreamsForNewMapping = () => {
  const connection = useCurrentConnection();
  const { streamsWithMappings } = useMappingContext();

  return connection?.syncCatalog.streams?.filter((stream) => {
    if (!stream.stream?.name) {
      return false;
    }

    if (
      streamsWithMappings[getKeyForStream(stream.stream)] &&
      streamsWithMappings[getKeyForStream(stream.stream)].length > 0
    ) {
      return false;
    }

    if (!stream.config?.selected) {
      return false;
    }

    return true;
  });
};
