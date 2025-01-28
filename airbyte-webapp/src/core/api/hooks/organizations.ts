import { useMutation, useQueryClient } from "@tanstack/react-query";

import { useCurrentWorkspaceId } from "area/workspace/utils";

import { useGetWorkspace } from "./workspaces";
import {
  getOrganization,
  getOrganizationInfo,
  listUsersInOrganization,
  updateOrganization,
  getOrganizationTrialStatus,
  getOrganizationUsage,
} from "../generated/AirbyteClient";
import { OrganizationUpdateRequestBody } from "../generated/AirbyteClient.schemas";
import { SCOPE_ORGANIZATION, SCOPE_USER } from "../scopes";
import { ConsumptionTimeWindow, OrganizationTrialStatusRead, OrganizationUserReadList } from "../types/AirbyteClient";
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
  trialStatus: (organizationId: string) => [SCOPE_ORGANIZATION, "trial", organizationId] as const,
  usage: (organizationId: string, timeWindow: string) =>
    [SCOPE_ORGANIZATION, "usage", organizationId, timeWindow] as const,
};

/**
 * Returns the organization of the workspace the user is currently in. Throws an error if the
 * user isn't inside a workspace.
 */
export const useCurrentOrganizationInfo = () => {
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();

  if (!workspaceId) {
    throw new Error(`Called useCurrentOrganizationInfo outside of a workspace`);
  }

  const workspace = useGetWorkspace(workspaceId);

  return useSuspenseQuery(organizationKeys.info(workspace.organizationId), () =>
    getOrganizationInfo({ workspaceId: workspace.workspaceId }, requestOptions)
  );
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

export const useListUsersInOrganization = (organizationId?: string): OrganizationUserReadList => {
  const requestOptions = useRequestOptions();
  const queryKey = organizationKeys.listUsers(organizationId ?? "");

  return (
    useSuspenseQuery(
      queryKey,
      () => listUsersInOrganization({ organizationId: organizationId ?? "" }, requestOptions),
      {
        enabled: !!organizationId,
      }
    ) ?? {
      users: [],
    }
  );
};

export const useOrganizationTrialStatus = (
  organizationId: string,
  enabled: boolean
): OrganizationTrialStatusRead | undefined => {
  const requestOptions = useRequestOptions();
  return useSuspenseQuery(
    organizationKeys.trialStatus(organizationId),
    () => {
      return getOrganizationTrialStatus({ organizationId }, requestOptions);
    },
    { enabled }
  );
};

export const useOrganizationUsage = ({ timeWindow }: { timeWindow: ConsumptionTimeWindow }) => {
  const requestOptions = useRequestOptions();
  const { organizationId } = useCurrentOrganizationInfo();

  return useSuspenseQuery(organizationKeys.usage(organizationId, timeWindow), () =>
    getOrganizationUsage({ organizationId, timeWindow }, requestOptions)
  );
};
