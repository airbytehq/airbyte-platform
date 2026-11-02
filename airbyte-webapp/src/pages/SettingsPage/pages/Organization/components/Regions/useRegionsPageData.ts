import { useMemo } from "react";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useListDataplaneGroups, useListDataplaneHealth, useListWorkspacesInOrganization } from "core/api";

import { buildRegions, Region } from "./regionsPageData";

// The workspaces query does not suspend, so the regions can render before their workspaces are known.
export type WorkspacesStatus = "loading" | "error" | "ready";

// The workload launcher checks in every 30 seconds, so polling faster only repeats the same answer.
const HEALTH_REFETCH_INTERVAL_MS = 30_000;

export const useRegionsPageData = (): { regions: Region[]; workspacesStatus: WorkspacesStatus } => {
  const organizationId = useCurrentOrganizationId();
  const dataplaneGroups = useListDataplaneGroups();
  const workspacesQuery = useListWorkspacesInOrganization({ organizationId });
  const { data: dataplanes } = useListDataplaneHealth({ refetchInterval: HEALTH_REFETCH_INTERVAL_MS });

  const regions = useMemo(
    () => buildRegions(dataplaneGroups, dataplanes, workspacesQuery.data?.pages[0].workspaces ?? [], organizationId),
    [dataplaneGroups, dataplanes, workspacesQuery.data, organizationId]
  );

  return { regions, workspacesStatus: workspacesStatusOf(workspacesQuery) };
};

const workspacesStatusOf = ({ isLoading, isError }: { isLoading: boolean; isError: boolean }): WorkspacesStatus => {
  if (isLoading) {
    return "loading";
  }
  if (isError) {
    return "error";
  }
  return "ready";
};
