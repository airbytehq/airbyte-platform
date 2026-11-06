import { HealthFilter, Region, RegionFilters, RegionTypeFilter } from "../types";

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
