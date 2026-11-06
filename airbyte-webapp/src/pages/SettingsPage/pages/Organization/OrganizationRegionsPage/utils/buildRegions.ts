import { DataplaneGroupRead, DataplaneHealthRead, WorkspaceRead } from "core/api/types/AirbyteClient";

import { isAirbyteManagedRegion } from "./isAirbyteManagedRegion";
import { Region } from "../types";

export const buildRegions = (
  groups: DataplaneGroupRead[],
  dataplanes: DataplaneHealthRead[],
  workspaces: WorkspaceRead[],
  organizationId: string
): Region[] => {
  const regionsById = new Map<string, Region>(
    groups.map((group) => [
      group.dataplane_group_id,
      {
        regionId: group.dataplane_group_id,
        name: group.name,
        isAirbyteManaged: isAirbyteManagedRegion(group, organizationId),
        dataplanes: [],
        workspaces: [],
      },
    ])
  );

  dataplanes.forEach((dataplane) => {
    regionsById.get(dataplane.dataplane_group_id)?.dataplanes.push(dataplane);
  });

  workspaces.forEach((workspace) => {
    if (!workspace.dataplaneGroupId) {
      return;
    }
    regionsById
      .get(workspace.dataplaneGroupId)
      ?.workspaces.push({ workspaceId: workspace.workspaceId, name: workspace.name });
  });

  return Array.from(regionsById.values());
};
