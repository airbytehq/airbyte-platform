import { DataplaneGroupRead, DataplaneHealthRead, WorkspaceRead } from "core/api/types/AirbyteClient";

export interface RegionWorkspace {
  workspaceId: string;
  name: string;
}

export interface Region {
  regionId: string;
  name: string;
  isAirbyteManaged: boolean;
  dataplanes: DataplaneHealthRead[];
  workspaces: RegionWorkspace[];
}

export type RegionTypeFilter = "all" | "airbyteManaged" | "selfManaged";
export type HealthFilter = "all" | DataplaneHealthRead["status"];

export interface RegionFilters {
  search: string;
  regionType: RegionTypeFilter;
  health: HealthFilter;
}

// The groups list holds the organization's own regions plus shared ones owned by Airbyte
// (the default organization and, with the entitlement, the PrivateLink organization).
export const isAirbyteManagedRegion = (
  group: Pick<DataplaneGroupRead, "organization_id">,
  organizationId: string
): boolean => group.organization_id !== organizationId;

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

const matchesSearch = (region: Region, search: string): boolean => {
  const needle = search.trim().toLocaleLowerCase();
  if (needle === "") {
    return true;
  }
  const names = [
    region.name,
    ...region.dataplanes.map((dataplane) => dataplane.dataplane_name),
    ...region.workspaces.map((workspace) => workspace.name),
  ];
  return names.some((name) => name.toLocaleLowerCase().includes(needle));
};

const matchesRegionType = (region: Region, regionType: RegionTypeFilter): boolean => {
  switch (regionType) {
    case "all":
      return true;
    case "airbyteManaged":
      return region.isAirbyteManaged;
    case "selfManaged":
      return !region.isAirbyteManaged;
  }
};

// Health of Airbyte-managed data planes is not exposed, so any health filter excludes those regions.
const matchesHealth = (region: Region, health: HealthFilter): boolean => {
  if (health === "all") {
    return true;
  }
  if (region.isAirbyteManaged) {
    return false;
  }
  return region.dataplanes.some((dataplane) => dataplane.status === health);
};

export const filterRegions = (regions: Region[], filters: RegionFilters): Region[] =>
  regions
    .filter(
      (region) =>
        matchesSearch(region, filters.search) &&
        matchesRegionType(region, filters.regionType) &&
        matchesHealth(region, filters.health)
    )
    .sort((a, b) => a.name.localeCompare(b.name, undefined, { sensitivity: "base" }));
