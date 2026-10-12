import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { useCurrentOrganizationId } from "area/organization/utils";

import {
  getOrganizationSemanticSearchEnablement,
  updateOrganizationSemanticSearchEnablement,
} from "../generated/AirbyteClient";
import { SCOPE_ORGANIZATION } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";

export const organizationSemanticSearchKeys = {
  all: (organizationId: string) => [SCOPE_ORGANIZATION, "semanticSearch", organizationId] as const,
};

export const useOrganizationSemanticSearchEnablement = () => {
  const organizationId = useCurrentOrganizationId();
  const requestOptions = useRequestOptions();

  return useQuery(
    organizationSemanticSearchKeys.all(organizationId),
    () => getOrganizationSemanticSearchEnablement({ organizationId }, requestOptions),
    {
      enabled: Boolean(organizationId),
      retry: false,
      staleTime: 30_000,
    }
  );
};

export const useUpdateOrganizationSemanticSearchEnablement = () => {
  const organizationId = useCurrentOrganizationId();
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation(
    (semanticSearchEnabled: boolean) =>
      updateOrganizationSemanticSearchEnablement({ organizationId, semanticSearchEnabled }, requestOptions),
    {
      onSuccess: async (response) => {
        queryClient.setQueryData(organizationSemanticSearchKeys.all(response.organizationId), response);
        await queryClient.invalidateQueries(organizationSemanticSearchKeys.all(response.organizationId));
      },
    }
  );
};
