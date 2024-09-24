import { useMutation, useQueryClient } from "@tanstack/react-query";

import { ApiCallOptions } from "../apiCall";
import { getInstanceConfiguration, setupInstanceConfiguration, licenseInfo } from "../generated/AirbyteClient";
import { InstanceConfigurationSetupRequestBody } from "../generated/AirbyteClient.schemas";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

const QUERY_KEY_INSTANCE = "airbyte-instance";
const QUERY_KEY_LICENSE = "airbyte-license";

export function useGetInstanceConfiguration() {
  // The instance configuration endpoint is not authenticated, so we don't need to pass any auth headers.
  // But because of how the API client is generated, we still need to pass an empty request options object.
  const emptyRequestOptions: ApiCallOptions = { getAccessToken: () => Promise.resolve(null) };

  return useSuspenseQuery([QUERY_KEY_INSTANCE], () => getInstanceConfiguration(emptyRequestOptions), {
    staleTime: Infinity, // This data should be fetched once and never updated
  });
}

export function useSetupInstanceConfiguration() {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  return useMutation(
    (body: InstanceConfigurationSetupRequestBody) => setupInstanceConfiguration(body, requestOptions),
    {
      onSuccess: (data) => {
        queryClient.setQueryData([QUERY_KEY_INSTANCE], data);
      },
    }
  );
}

export const useGetLicenseDetails = () => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery([QUERY_KEY_LICENSE], () => {
    return licenseInfo(requestOptions);
  });
};
