import { getInstanceConfiguration } from "../generated/AirbyteClient";
import { useSuspenseQuery } from "../useSuspenseQuery";

export function useGetInstanceConfiguration() {
  // The instance configuration endpoint is not authenticated, so we don't need to pass any auth headers.
  // But because of how the API client is generated, we still need to pass an empty request options object.
  const emptyRequestOptions = { middlewares: [] };

  return useSuspenseQuery(["airbyte-instance"], () => getInstanceConfiguration(emptyRequestOptions), {
    staleTime: Infinity, // This data should be fetched once and never updated
  });
}
