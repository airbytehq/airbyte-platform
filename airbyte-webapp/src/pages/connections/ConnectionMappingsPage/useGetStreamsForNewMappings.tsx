import { useCurrentConnection } from "core/api";

export const useGetStreamsForNewMapping = () => {
  const connection = useCurrentConnection();

  return connection?.syncCatalog.streams?.filter((stream) => {
    if (stream.config?.mappers && stream.config?.mappers.length > 0) {
      return false;
    }

    if (!stream.config?.selected) {
      return false;
    }

    return true;
  });
};
