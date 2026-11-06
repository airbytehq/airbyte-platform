import { DataplaneHealthRead } from "core/api/types/AirbyteClient";

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

// The workspaces query does not suspend, so the regions can render before their workspaces are known.
export type WorkspacesStatus = "loading" | "error" | "ready";
