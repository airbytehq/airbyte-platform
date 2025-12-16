import {
  DataplaneGroupRead,
  OrganizationDataWorkerUsageRead,
  RegionDataWorkerUsage,
} from "core/api/types/AirbyteClient";

import styles from "./UsageByWorkspaceGraph.module.scss";

const WORKSPACE_COLORS = [
  styles.blue800, // #300ad6 - Dark blue
  styles.pink500, // #e94ea2 - Bright pink
  styles.cyan700, // #008f99 - Teal
  styles.orange200, // #fea895 - Light coral
  styles.blue400, // #605cff - Blue-purple
  styles.pink700, // #b600b6 - Magenta
  styles.purple400, // #ac56e9 - Light purple
  styles.blueBright, // Bright blue
  styles.pink200, // #ff8fa3 - Light pink
  styles.cyan500, // #07becd - Cyan
] as const;

/**
 * Get the color for a workspace based on its index.
 */
export function getWorkspaceColorByIndex(index: number): string {
  return WORKSPACE_COLORS[index % WORKSPACE_COLORS.length];
}

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
