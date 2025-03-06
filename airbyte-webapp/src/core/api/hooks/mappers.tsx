import { useQueryClient } from "@tanstack/react-query";

import { useCurrentConnection } from "./connections";
import { webBackendValidateMappers } from "../generated/AirbyteClient";
import { SCOPE_WORKSPACE } from "../scopes";
import { ConfiguredStreamMapper, StreamDescriptor, WebBackendValidateMappersResponse } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const mappersKeys = {
  all: [SCOPE_WORKSPACE, "mappers"] as const,
  validation: (connectionId: string, streamDescriptor: StreamDescriptor) => [
    SCOPE_WORKSPACE,
    "mappers",
    connectionId,
    `${streamDescriptor.namespace}-${streamDescriptor.name}`,
  ],
};
export const useValidateMappers = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const { connectionId } = useCurrentConnection();

  const fetchQuery = (streamDescriptor: StreamDescriptor, mappers: ConfiguredStreamMapper[]) => {
    const queryKey = mappersKeys.validation(connectionId, streamDescriptor);

    return queryClient.fetchQuery<WebBackendValidateMappersResponse>(queryKey, () =>
      webBackendValidateMappers({ connectionId, streamDescriptor, mappers }, requestOptions)
    );
  };

  const getCachedData = (streamDescriptor: StreamDescriptor) => {
    const queryKey = ["mappers", connectionId, `${streamDescriptor.namespace}-${streamDescriptor.name}`];
    return queryClient.getQueryData<WebBackendValidateMappersResponse>(queryKey);
  };

  return { fetchQuery, getCachedData };
};

export const useInitialValidation = (streamDescriptor: StreamDescriptor, mappers: ConfiguredStreamMapper[]) => {
  const requestOptions = useRequestOptions();

  const { connectionId } = useCurrentConnection();
  const queryKey = mappersKeys.validation(connectionId, streamDescriptor);

  return useSuspenseQuery<WebBackendValidateMappersResponse>(queryKey, () =>
    webBackendValidateMappers({ connectionId, streamDescriptor, mappers }, requestOptions)
  );
};
