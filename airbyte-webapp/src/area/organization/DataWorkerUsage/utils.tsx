import {
  DataplaneGroupRead,
  OrganizationDataWorkerUsageRead,
  RegionDataWorkerUsage,
} from "core/api/types/AirbyteClient";

/**
 * Comparator function to sort items by name alphabetically.
 */
export const sortByNameAlphabetically = <T extends { name: string }>(a: T, b: T): number =>
  a.name.localeCompare(b.name);

/**
 * Predicate to check if a region has any workspace usage data.
 */
export const hasUsageData = (regionUsage: RegionDataWorkerUsage): boolean =>
  regionUsage.workspaces.some((ws) => ws.dataWorkers.length > 0);

/**
 * Creates a predicate function that checks if a dataplane group has usage in the provided usage data.
 */
export const createHasUsagePredicate =
  (allUsage?: OrganizationDataWorkerUsageRead) =>
  (region: DataplaneGroupRead): boolean => {
    const regionUsage = allUsage?.regions.find((r) => r.id === region.dataplane_group_id);
    return regionUsage ? hasUsageData(regionUsage) : false;
  };

/**
 * Finds the first region with usage data from a sorted list of regions.
 * Returns undefined if no region has usage.
 */
export const findFirstRegionWithUsage = (
  regions: DataplaneGroupRead[],
  allUsage?: OrganizationDataWorkerUsageRead
): DataplaneGroupRead | undefined => {
  const hasUsage = createHasUsagePredicate(allUsage);
  return regions.find(hasUsage);
};

/**
 * Transforms a DataplaneGroupRead into a ListBox option format.
 */
export const toRegionOption = (region: DataplaneGroupRead) => ({
  label: region.name,
  value: region.dataplane_group_id,
});

/**
 * Transforms an array of DataplaneGroupRead into ListBox options.
 */
export const getRegionOptions = (regions: DataplaneGroupRead[]) => regions.map(toRegionOption);
