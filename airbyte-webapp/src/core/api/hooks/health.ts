import { useQuery } from "@tanstack/react-query";

import { getHealthCheck } from "../generated/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";

const HEALTHCHECK_MAX_COUNT = 3;
const HEALTHCHECK_INTERVAL = 20000;

export const useHealthCheck = (onError: () => void, onSuccess: () => void) => {
  const requestOptions = useRequestOptions();

  const { failureCount } = useQuery(["healthCheck"], () => getHealthCheck(requestOptions), {
    refetchInterval: HEALTHCHECK_INTERVAL,
    retry: HEALTHCHECK_MAX_COUNT,
    retryDelay: HEALTHCHECK_INTERVAL,
    onError: () => {
      if (failureCount >= HEALTHCHECK_MAX_COUNT) {
        onError();
      }
    },
    onSuccess,
  });
};
