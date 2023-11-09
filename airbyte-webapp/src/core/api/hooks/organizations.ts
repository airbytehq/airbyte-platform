import { useMutation, useQueryClient } from "@tanstack/react-query";

import { SCOPE_ORGANIZATION, SCOPE_USER } from "services/Scope";

import { useCurrentWorkspace } from "./workspaces";
import {
  getOrganization,
  getOrganizationInfo,
  listUsersInOrganization,
  updateOrganization,
} from "../generated/AirbyteClient";
import { OrganizationUpdateRequestBody } from "../generated/AirbyteClient.schemas";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const organizationKeys = {
  all: [SCOPE_USER, "organizations"] as const,
  lists: () => [...organizationKeys.all, "list"] as const,
  list: (filters: string[]) => [...organizationKeys.lists(), filters] as const,
  detail: (organizationId = "<none>") => [...organizationKeys.all, "details", organizationId] as const,
  allListUsers: [SCOPE_ORGANIZATION, "users", "list"] as const,
  listUsers: (organizationId: string) => [SCOPE_ORGANIZATION, "users", "list", organizationId] as const,
};

/**
 * Returns the organization of the workspace the user is currently in. Will return `null` if the
 * user isn't inside a workspace or the workspace doesn't belong to an organization.
 */
export const useCurrentOrganizationInfo = () => {
  const requestOptions = useRequestOptions();
  const workspace = useCurrentWorkspace();
  return useSuspenseQuery(organizationKeys.detail(workspace.organizationId), () => {
    // TODO: Once all workspaces are in an organization this can be removed, but for now
    //       we guard against calling the endpoint if the workspace isn't in an organization
    //       to not cause too many 404 in the getOrganizationInfo endpoint.
    if (!workspace.organizationId) {
      return Promise.resolve(null);
    }

    return getOrganizationInfo({ workspaceId: workspace.workspaceId }, requestOptions);
  });
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

export const useListOrganizationsById = (organizationIds: string[]) => {
  const requestOptions = useRequestOptions();
  const queryKey = organizationKeys.list(organizationIds);

  return useSuspenseQuery(queryKey, () =>
    Promise.all(organizationIds.map((organizationId) => getOrganization({ organizationId }, requestOptions)))
  );
};

export const useListUsersInOrganization = (organizationId: string) => {
  const requestOptions = useRequestOptions();
  const queryKey = organizationKeys.listUsers(organizationId);

  return useSuspenseQuery(queryKey, () => listUsersInOrganization({ organizationId }, requestOptions));
};
