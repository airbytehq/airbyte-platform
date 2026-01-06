import { useMemo } from "react";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useListDataplaneGroups, useListDataplaneHealth, useListWorkspacesInOrganization } from "core/api";

export interface DataplaneWithHealth {
  dataplane_id: string;
  dataplane_name: string;
  status: string;
  last_heartbeat_timestamp?: number;
  recent_heartbeats?: Array<{ timestamp: number }>;
  dataplane_version?: string;
}

export interface WorkspaceInfo {
  workspace_id: string;
  workspace_name: string;
}

export interface RegionData {
  region_id: string;
  region_name: string;
  dataplanes: DataplaneWithHealth[];
  workspaces: WorkspaceInfo[];
}

export const useNestedRegionsData = () => {
  const organizationId = useCurrentOrganizationId();
  const dataplaneGroups = useListDataplaneGroups();
  const { data: workspacesData } = useListWorkspacesInOrganization({ organizationId });
  const { data: healthData } = useListDataplaneHealth();

  const regionsData = useMemo(() => {
    const workspaces = workspacesData?.pages[0].workspaces ?? [];

    // Create a map of region_id -> RegionData
    const regionsMap = new Map<string, RegionData>();

    // Initialize regions from dataplane groups
    dataplaneGroups?.forEach((group) => {
      regionsMap.set(group.dataplane_group_id, {
        region_id: group.dataplane_group_id,
        region_name: group.name,
        dataplanes: [],
        workspaces: [],
      });
    });

    // Add dataplanes with health data
    healthData.forEach((health) => {
      const regionData = regionsMap.get(health.dataplane_group_id);
      if (regionData) {
        regionData.dataplanes.push({
          dataplane_id: health.dataplane_id,
          dataplane_name: health.dataplane_name,
          status: health.status,
          last_heartbeat_timestamp: health.last_heartbeat_timestamp ?? undefined,
          recent_heartbeats: health.recent_heartbeats,
          dataplane_version: health.dataplane_version,
        });
      }
    });

    // Add workspaces to their respective regions
    workspaces.forEach((workspace) => {
      if (workspace.dataplaneGroupId) {
        const regionData = regionsMap.get(workspace.dataplaneGroupId);
        if (regionData) {
          regionData.workspaces.push({
            workspace_id: workspace.workspaceId,
            workspace_name: workspace.name,
          });
        }
      }
    });

    return Array.from(regionsMap.values());
  }, [dataplaneGroups, workspacesData, healthData]);

  return { regionsData };
};
