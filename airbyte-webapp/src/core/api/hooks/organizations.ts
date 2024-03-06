import { useMutation, useQueryClient } from "@tanstack/react-query";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useAuthService } from "core/services/auth";

import { useGetWorkspace } from "./workspaces";
import {
  getOrganization,
  getOrganizationInfo,
  listUsersInOrganization,
  updateOrganization,
} from "../generated/AirbyteClient";
import { OrganizationUpdateRequestBody } from "../generated/AirbyteClient.schemas";
import { SCOPE_ORGANIZATION, SCOPE_USER } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const organizationKeys = {
  all: [SCOPE_USER, "organizations"] as const,
  lists: () => [...organizationKeys.all, "list"] as const,
  list: (filters: string[]) => [...organizationKeys.lists(), filters] as const,
  info: (organizationId = "<none>") => [...organizationKeys.all, "info", organizationId] as const,
  detail: (organizationId = "<none>") => [...organizationKeys.all, "details", organizationId] as const,
  allListUsers: [SCOPE_ORGANIZATION, "users", "list"] as const,
  listUsers: (organizationId: string) => [SCOPE_ORGANIZATION, "users", "list", organizationId] as const,
};

/**
 * Returns the organization of the workspace the user is currently in. Will return `null` if the
 * user isn't inside a workspace or the workspace doesn't belong to an organization.
 */
export const useCurrentOrganizationInfo = () => {
  const { user } = useAuthService();
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();

  // Because this hook is called before the auth service initializes (because we want to add the organization to the LDEXperimentService)
  // the user might be null. In that case, we should disable the query, otherwise it will fail due to no valid JWT being present yet.
  const workspace = useGetWorkspace(workspaceId, { enabled: !!workspaceId && !!user });

  return useSuspenseQuery(organizationKeys.info(workspace?.organizationId ?? ""), () => {
    // TODO: Once all workspaces are in an organization this can be removed, but for now
    //       we guard against calling the endpoint if the workspace isn't in an organization
    //       to not cause too many 404 in the getOrganizationInfo endpoint.
    if (!workspace?.organizationId || !user) {
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

export const useListUsersInOrganization = (organizationId: string, enabled: boolean = true) => {
  const requestOptions = useRequestOptions();
  const queryKey = organizationKeys.listUsers(organizationId);

  return useSuspenseQuery(queryKey, () => listUsersInOrganization({ organizationId }, requestOptions), { enabled });
};
