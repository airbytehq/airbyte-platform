import { useMutation, useQueryClient, useQuery, UseInfiniteQueryResult, useInfiniteQuery } from "@tanstack/react-query";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useCurrentUser } from "core/services/auth";

import { getWorkspaceQueryKey } from "./workspaces";
import { ApiCallOptions } from "../apiCall";
import {
  getOrganization,
  getOrgInfo,
  listUsersInOrganization,
  updateOrganization,
  getOrganizationTrialStatus,
  getOrganizationUsage,
  listWorkspacesInOrganization,
  listOrganizationsByUser,
  listOrganizationSummaries,
} from "../generated/AirbyteClient";
import { OrganizationUpdateRequestBody } from "../generated/AirbyteClient.schemas";
import {
  listInternalAccountOrganizations,
  getEmbeddedOrganizationsCurrentScoped,
  createInternalAccountOrganizationsIdOnboardingProgress,
} from "../generated/SonarClient";
import { SCOPE_ORGANIZATION, SCOPE_USER } from "../scopes";
import {
  ConsumptionTimeWindow,
  ListOrganizationsByUserRequestBody,
  ListOrganizationSummariesRequestBody,
  OrganizationRead,
  OrganizationTrialStatusRead,
  OrganizationUserReadList,
  OrganizationInfoRead,
  ListOrganizationSummariesResponse,
  WorkspaceReadList,
  ListWorkspacesInOrganizationRequestBody,
  WorkspaceRead,
} from "../types/AirbyteClient";
import { OnboardingStatusEnum, Organization } from "../types/SonarClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";
export const organizationKeys = {
  all: [SCOPE_USER, "organizations"] as const,
  lists: () => [...organizationKeys.all, "list"] as const,
  list: (filters: string[]) => [...organizationKeys.lists(), filters] as const,
  info: (organizationId = "<none>") => [...organizationKeys.all, "info", organizationId] as const,
  detail: (organizationId = "<none>") => [...organizationKeys.all, "details", organizationId] as const,
  orgInfo: (organizationId = "<none>") => [...organizationKeys.all, "orgInfo", organizationId] as const,
  allListUsers: [SCOPE_ORGANIZATION, "users", "list"] as const,
  listUsers: (organizationId: string) => [SCOPE_ORGANIZATION, "users", "list", organizationId] as const,
  trialStatus: (organizationId: string) => [SCOPE_ORGANIZATION, "trial", organizationId] as const,
  usage: (organizationId: string, timeWindow: string) =>
    [SCOPE_ORGANIZATION, "usage", organizationId, timeWindow] as const,
  workspacesList: (organizationId: string) => [SCOPE_ORGANIZATION, "workspaces", "list", organizationId] as const,
  workspaces: (organizationId: string, pageSize: number, nameContains?: string) =>
    [...organizationKeys.workspacesList(organizationId), pageSize, nameContains] as const,
  firstWorkspace: (organizationId: string) => [SCOPE_ORGANIZATION, organizationId, "workspaces", "first"] as const,
  listByUser: (requestBody: ListOrganizationsByUserRequestBody) =>
    [...organizationKeys.all, "byUser", requestBody] as const,
  summaries: (requestBody: ListOrganizationSummariesRequestBody) =>
    [...organizationKeys.all, "summaries", requestBody] as const,
  scopedTokenOrganization: () => [...organizationKeys.all, "scopedTokenOrganization"] as const,
};

export const useCurrentOrganizationInfo = () => {
  const requestOptions = useRequestOptions();
  const organizationId = useCurrentOrganizationId();

  return useSuspenseQuery(organizationKeys.info(organizationId), () => getOrgInfo({ organizationId }, requestOptions));
};

export const useOrganization = (organizationId: string) => {
  const requestOptions = useRequestOptions();
  return useSuspenseQuery(organizationKeys.detail(organizationId), () =>
    getOrganization({ organizationId }, requestOptions)
  );
};

export const useOrgInfo = (organizationId: string, enabled?: boolean): OrganizationInfoRead | undefined => {
  const requestOptions = useRequestOptions();
  return useSuspenseQuery(
    organizationKeys.orgInfo(organizationId),
    () => getOrgInfo({ organizationId }, requestOptions),
    {
      enabled,
    }
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

const TRIAL_STATUS_REFETCH_INTERVAL = 60 * 60 * 1000; // 1 hour in milliseconds

export const useOrganizationTrialStatus = (
  organizationId: string,
  options?: { refetchInterval?: boolean; enabled?: boolean; onSuccess?: (data: OrganizationTrialStatusRead) => void }
): OrganizationTrialStatusRead | undefined => {
  const requestOptions = useRequestOptions();
  return useSuspenseQuery(
    organizationKeys.trialStatus(organizationId),
    () => {
      return getOrganizationTrialStatus({ organizationId }, requestOptions);
    },
    {
      enabled: options?.enabled,
      refetchInterval: options?.refetchInterval ? TRIAL_STATUS_REFETCH_INTERVAL : undefined,
      onSuccess: options?.onSuccess,
    }
  );
};

export const useOrganizationUsage = ({ timeWindow }: { timeWindow: ConsumptionTimeWindow }) => {
  const requestOptions = useRequestOptions();
  const organizationId = useCurrentOrganizationId();

  return useSuspenseQuery(organizationKeys.usage(organizationId, timeWindow), () =>
    getOrganizationUsage({ organizationId, timeWindow }, requestOptions)
  );
};

// Sometimes we need to get the first workspace in and organization, for example if the URL only contains an
// organization id, but we still want to populate the workspace selector with a default workspace.
export const useDefaultWorkspaceInOrganization = (organizationId: string, enabled: boolean = true) => {
  const queryClient = useQueryClient();
  const requestOptions = useRequestOptions();

  return useSuspenseQuery<WorkspaceRead | null | undefined>(
    organizationKeys.firstWorkspace(organizationId),
    async () => {
      const { workspaces } = await listWorkspacesInOrganization(
        { organizationId, pagination: { pageSize: 1, rowOffset: 0 } },
        requestOptions
      );
      const firstWorkspace = workspaces[0];
      // We can set the query data for this workspace id at this point. This will mean that the cache is already
      // populated for calls to useGetWorkspace(firstWorkspace.workspaceId). This is particularly useful in the sidebar,
      // since we want to show the first workspace by default when the user selects an organization. This effectively
      // pre-fetches the workspace in that case.
      if (firstWorkspace) {
        queryClient.setQueryData(getWorkspaceQueryKey(firstWorkspace.workspaceId), firstWorkspace);
      }
      // It is possible that there are no workspaces in an organization. react-query will throw if we return
      // undefined, so we set null to indicate the lack of a default workspace
      return firstWorkspace ?? null;
    },
    { enabled, staleTime: 30_000 }
  );
};

export const useListWorkspacesInOrganization = ({
  organizationId,
  pagination,
  nameContains,
  enabled = true,
}: ListWorkspacesInOrganizationRequestBody & { enabled?: boolean }): UseInfiniteQueryResult<
  WorkspaceReadList,
  unknown
> => {
  const requestOptions = useRequestOptions();
  const pageSize = pagination?.pageSize ?? 10;
  const queryKey = organizationKeys.workspaces(organizationId, pageSize, nameContains?.trim());

  const listWorkspacesQueryFn = async ({ pageParam = 0 }) => {
    const rowOffset = pageParam * pageSize;
    return listWorkspacesInOrganization(
      {
        organizationId,
        pagination: pagination ? { pageSize, rowOffset } : undefined,
        nameContains,
      },
      requestOptions
    );
  };

  return useInfiniteQuery({
    enabled,
    queryKey,
    queryFn: listWorkspacesQueryFn,
    getNextPageParam: (lastPage, allPages) => {
      const pageSize = pagination?.pageSize ?? 10;
      const workspaces = lastPage.workspaces ?? [];
      return workspaces.length < pageSize ? undefined : allPages.length;
    },
    getPreviousPageParam: (firstPage, allPages) => {
      const pageSize = pagination?.pageSize ?? 10;
      const workspaces = firstPage.workspaces ?? [];
      return workspaces.length < pageSize ? undefined : allPages.length - 1;
    },
  });
};

export const useListOrganizationsByUser = (requestBody: ListOrganizationsByUserRequestBody) => {
  const requestOptions = useRequestOptions();
  return useSuspenseQuery(
    organizationKeys.listByUser(requestBody),
    () => listOrganizationsByUser(requestBody, requestOptions),
    {
      staleTime: 30_0000,
    }
  );
};

export const useListOrganizationsByUserInfinite = (
  requestBody: Omit<ListOrganizationSummariesRequestBody, "pagination">,
  pageSize = 10
) => {
  const requestOptions = useRequestOptions();
  return useInfiniteQuery({
    queryKey: organizationKeys.listByUser({ ...requestBody, pagination: { pageSize } }),
    queryFn: async ({ pageParam = 0 }) =>
      listOrganizationsByUser(
        { ...requestBody, pagination: { pageSize, rowOffset: pageParam * pageSize } },
        requestOptions
      ),
    staleTime: 1000 * 60 * 5,
    cacheTime: 1000 * 60 * 30,
    getPreviousPageParam: (firstPage, allPages) =>
      firstPage.organizations.length > 0 ? allPages.length - 1 : undefined,
    getNextPageParam: (lastPage, allPages) => (lastPage.organizations.length < pageSize ? undefined : allPages.length),
  });
};

const listOrgSummariesQueryFn =
  (requestBody: ListOrganizationSummariesRequestBody, requestOptions: ApiCallOptions) =>
  async ({ pageParam = 0 }) => {
    const pageSize = requestBody.pagination.pageSize ?? 10;
    const rowOffset = pageParam * pageSize;
    return listOrganizationSummaries(
      { ...requestBody, pagination: { ...requestBody.pagination, rowOffset } },
      requestOptions
    );
  };

export const useListOrganizationSummaries = (
  requestBody: ListOrganizationSummariesRequestBody
): UseInfiniteQueryResult<ListOrganizationSummariesResponse, unknown> => {
  const requestOptions = useRequestOptions();

  return useInfiniteQuery({
    queryKey: organizationKeys.summaries(requestBody),
    queryFn: listOrgSummariesQueryFn(requestBody, requestOptions),
    suspense: false,
    staleTime: 1000 * 60 * 5,
    cacheTime: 1000 * 60 * 30,
    refetchOnWindowFocus: false,
    refetchOnMount: false,
    getNextPageParam: (lastPage, allPages) => {
      const pageSize = requestBody.pagination.pageSize ?? 10;
      const summaries = lastPage.organizationSummaries ?? [];
      return summaries.length < pageSize ? undefined : allPages.length;
    },
    getPreviousPageParam: (firstPage, allPages) => {
      const pageSize = requestBody.pagination.pageSize ?? 10;
      const summaries = firstPage.organizationSummaries ?? [];
      return summaries.length < pageSize ? undefined : allPages.length - 1;
    },
  });
};

export const usePrefetchOrganizationSummaries = () => {
  const queryClient = useQueryClient();
  const { userId } = useCurrentUser();
  const requestOptions = useRequestOptions();
  const requestBody: ListOrganizationSummariesRequestBody = {
    userId,
    nameContains: "",
    pagination: { pageSize: 10 },
  };

  return () => {
    return queryClient.prefetchInfiniteQuery({
      queryKey: organizationKeys.summaries(requestBody),
      queryFn: listOrgSummariesQueryFn(requestBody, requestOptions),
      staleTime: 1000 * 60 * 5,
      cacheTime: 1000 * 60 * 30,
    });
  };
};

export const useFirstOrg = (): OrganizationRead => {
  const { userId } = useCurrentUser();
  const { organizations } = useListOrganizationsByUser({ userId });
  // NOTE: Users invited to a workspace may have no organization.
  // https://github.com/airbytehq/airbyte-internal-issues/issues/13034
  return organizations[0] || {};
};

export const useOrganizationUserCount = (organizationId: string): number | null => {
  const requestOptions = useRequestOptions();
  return (
    useQuery(
      [SCOPE_ORGANIZATION, "users", "count", organizationId],
      async () => {
        return listUsersInOrganization({ organizationId }, requestOptions);
      },
      {
        select: (data) => data.users.length,
      }
    ).data ?? null
  );
};

export const useGetScopedOrganization = () => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(
    organizationKeys.scopedTokenOrganization(),
    () => getEmbeddedOrganizationsCurrentScoped(requestOptions),
    {
      staleTime: Infinity,
    }
  );
};

export const useListEmbeddedOrganizations = () => {
  const requestOptions = useRequestOptions();
  return useSuspenseQuery(organizationKeys.list(["embedded"]), () => listInternalAccountOrganizations(requestOptions));
};

export const useUpdateEmbeddedOnboardingStatus = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation(
    ({ organizationId, status }: { organizationId: string; status: OnboardingStatusEnum }) => {
      return createInternalAccountOrganizationsIdOnboardingProgress(
        organizationId,
        { onboarding_status: status },
        requestOptions
      );
    },
    {
      onSuccess: (data) => {
        queryClient.setQueryData<Organization | undefined>(organizationKeys.detail(data.organization_id), (old) => {
          if (!old) {
            return undefined;
          }
          return {
            ...old,
            onboarding_status: data.onboarding_progress ?? old.onboarding_status,
          };
        });
      },
    }
  );
};
