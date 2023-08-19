import { useMutation, useQueryClient } from "@tanstack/react-query";

import { SCOPE_USER } from "services/Scope";

import { getOrganization, updateOrganization } from "../generated/AirbyteClient";
import { OrganizationUpdateRequestBody } from "../generated/AirbyteClient.schemas";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const organizationKeys = {
  all: [SCOPE_USER, "organizations"] as const,
  detail: (organizationId: string) => [...organizationKeys.all, "details", organizationId] as const,
};

export const useOrganization = (organizationId: string) => {
  const requestOptions = useRequestOptions();
  return useSuspenseQuery(organizationKeys.detail(organizationId), () =>
    getOrganization({ organizationId }, requestOptions)
  );
};

export const useUpdateOrganization = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation(
    (organization: OrganizationUpdateRequestBody) => updateOrganization(organization, requestOptions),
    {
      onSuccess: (data) => {
        queryClient.setQueryData(organizationKeys.detail(data.organizationId), data);
      },
    }
  );
};
