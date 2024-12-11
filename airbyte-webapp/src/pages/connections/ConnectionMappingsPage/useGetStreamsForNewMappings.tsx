import { useCurrentConnection } from "core/api";

import { useMappingContext } from "./MappingContext";

export const useGetStreamsForNewMapping = () => {
  const connection = useCurrentConnection();
  const { streamsWithMappings } = useMappingContext();

  return connection?.syncCatalog.streams?.filter((stream) => {
    if (!stream.stream?.name) {
      return false;
    }

    if (streamsWithMappings[stream.stream?.name]) {
      return false;
    }

    if (!stream.config?.selected) {
      return false;
    }

    return true;
  });
};
