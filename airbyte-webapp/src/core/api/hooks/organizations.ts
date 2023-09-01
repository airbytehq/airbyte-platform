import { useMutation, useQueryClient } from "@tanstack/react-query";

import { SCOPE_ORGANIZATION, SCOPE_USER } from "services/Scope";

import { getOrganization, listUsersInOrganization, updateOrganization } from "../generated/AirbyteClient";
import { OrganizationUpdateRequestBody } from "../generated/AirbyteClient.schemas";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const organizationKeys = {
  all: [SCOPE_USER, "organizations"] as const,
  detail: (organizationId: string) => [...organizationKeys.all, "details", organizationId] as const,
  allListUsers: [SCOPE_ORGANIZATION, "users", "list"] as const,
  listUsers: (organizationId: string) => [SCOPE_ORGANIZATION, "users", "list", organizationId] as const,
  workspaces: (organizationIds: string[]) => [...organizationKeys.all, "workspaces", organizationIds] as const,
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

export const useListUsersInOrganization = (organizationId: string) => {
  const requestOptions = useRequestOptions();
  const queryKey = organizationKeys.listUsers(organizationId);

  return useSuspenseQuery(queryKey, () => listUsersInOrganization({ organizationId }, requestOptions));
};
