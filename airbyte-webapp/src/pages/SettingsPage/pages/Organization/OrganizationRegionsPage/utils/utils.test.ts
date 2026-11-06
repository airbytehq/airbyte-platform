import { DataplaneGroupRead, DataplaneHealthRead, WorkspaceRead } from "core/api/types/AirbyteClient";

import { buildRegions } from "./buildRegions";
import { filterRegions } from "./filterRegions";
import { isAirbyteManagedRegion } from "./isAirbyteManagedRegion";
import { Region, RegionFilters } from "../types";

const ORGANIZATION_ID = "11111111-1111-1111-1111-111111111111";
const DEFAULT_ORGANIZATION_ID = "00000000-0000-0000-0000-000000000000";
const PRIVATELINK_ORGANIZATION_ID = "cc9498c5-9cfd-482b-ac5a-ed867e39d52e";

const group = (overrides: Partial<DataplaneGroupRead>): DataplaneGroupRead => ({
  name: "us-east-1",
  dataplane_group_id: "group-1",
  organization_id: ORGANIZATION_ID,
  enabled: true,
  created_at: "2026-10-28T00:00:00Z",
  updated_at: "2026-10-28T00:00:00Z",
  dataplanes: [],
  ...overrides,
});

const dataplane = (overrides: Partial<DataplaneHealthRead>): DataplaneHealthRead => ({
  dataplane_id: "dp-1",
  dataplane_name: "us-east-1-flex-lab",
  dataplane_group_id: "group-1",
  organization_id: ORGANIZATION_ID,
  status: "HEALTHY",
  enabled: true,
  ...overrides,
});

const workspace = (overrides: Partial<WorkspaceRead>): WorkspaceRead =>
  ({
    workspaceId: "ws-1",
    customerId: "customer-1",
    name: "Production",
    slug: "production",
    initialSetupComplete: true,
    dataplaneGroupId: "group-1",
    ...overrides,
  }) as WorkspaceRead;

const region = (overrides: Partial<Region>): Region => ({
  regionId: "group-1",
  name: "us-east-1",
  isAirbyteManaged: false,
  dataplanes: [],
  workspaces: [],
  ...overrides,
});

const noFilters: RegionFilters = { search: "", regionType: "all", health: "all" };

describe("isAirbyteManagedRegion", () => {
  it("treats a region owned by another organization as Airbyte-managed", () => {
    expect(isAirbyteManagedRegion({ organization_id: DEFAULT_ORGANIZATION_ID }, ORGANIZATION_ID)).toBe(true);
    expect(isAirbyteManagedRegion({ organization_id: PRIVATELINK_ORGANIZATION_ID }, ORGANIZATION_ID)).toBe(true);
    expect(isAirbyteManagedRegion({ organization_id: ORGANIZATION_ID }, ORGANIZATION_ID)).toBe(false);
  });
});

describe("buildRegions", () => {
  it("groups data planes and workspaces under their region", () => {
    const regions = buildRegions(
      [group({}), group({ dataplane_group_id: "group-2", name: "eu-west-1" })],
      [dataplane({}), dataplane({ dataplane_id: "dp-2", dataplane_group_id: "group-2", dataplane_name: "eu" })],
      [workspace({}), workspace({ workspaceId: "ws-2", name: "Staging", dataplaneGroupId: "group-2" })],
      ORGANIZATION_ID
    );

    expect(regions).toHaveLength(2);
    expect(regions[0].dataplanes.map((d) => d.dataplane_id)).toEqual(["dp-1"]);
    expect(regions[0].workspaces).toEqual([{ workspaceId: "ws-1", name: "Production" }]);
    expect(regions[1].dataplanes.map((d) => d.dataplane_id)).toEqual(["dp-2"]);
    expect(regions[1].workspaces).toEqual([{ workspaceId: "ws-2", name: "Staging" }]);
  });

  it("keeps a region with no data planes and no workspaces", () => {
    const regions = buildRegions([group({})], [], [], ORGANIZATION_ID);

    expect(regions).toEqual([region({})]);
  });

  it("ignores a workspace whose region is unknown or missing", () => {
    const regions = buildRegions(
      [group({})],
      [],
      [
        workspace({ dataplaneGroupId: "missing-group" }),
        workspace({ workspaceId: "ws-2", dataplaneGroupId: undefined }),
      ],
      ORGANIZATION_ID
    );

    expect(regions[0].workspaces).toEqual([]);
  });

  it("ignores a data plane whose region is unknown", () => {
    const regions = buildRegions(
      [group({})],
      [dataplane({ dataplane_group_id: "missing-group" })],
      [],
      ORGANIZATION_ID
    );

    expect(regions[0].dataplanes).toEqual([]);
  });

  it("marks regions owned by the default or the PrivateLink organization as Airbyte-managed", () => {
    const regions = buildRegions(
      [
        group({ organization_id: DEFAULT_ORGANIZATION_ID, name: "US" }),
        group({ dataplane_group_id: "g2", organization_id: PRIVATELINK_ORGANIZATION_ID, name: "US PrivateLink" }),
        group({ dataplane_group_id: "g3" }),
      ],
      [],
      [],
      ORGANIZATION_ID
    );

    expect(regions.map((r) => r.isAirbyteManaged)).toEqual([true, true, false]);
  });
});

describe("filterRegions", () => {
  const us = region({
    regionId: "us",
    name: "US",
    isAirbyteManaged: true,
    workspaces: [{ workspaceId: "ws-us", name: "Marketing" }],
  });
  const euWest = region({
    regionId: "eu-west-1",
    name: "eu-west-1",
    dataplanes: [dataplane({ dataplane_name: "eu-west-1-lab", status: "DEGRADED" })],
  });
  const usEast = region({
    regionId: "us-east-1",
    name: "us-east-1",
    dataplanes: [
      dataplane({ dataplane_name: "us-east-1-flex-lab", status: "HEALTHY" }),
      dataplane({ dataplane_id: "dp-2", dataplane_name: "us-east-1-test", status: "UNKNOWN" }),
    ],
    workspaces: [{ workspaceId: "ws-1", name: "Production" }],
  });
  const regions = [usEast, us, euWest];

  it("returns every region in alphabetical order by default", () => {
    expect(filterRegions(regions, noFilters).map((r) => r.name)).toEqual(["eu-west-1", "US", "us-east-1"]);
  });

  it("matches part of a region name, case-insensitive", () => {
    expect(filterRegions(regions, { ...noFilters, search: "EAST" }).map((r) => r.name)).toEqual(["us-east-1"]);
  });

  it("matches part of a data plane name and returns the whole region", () => {
    const result = filterRegions(regions, { ...noFilters, search: "flex" });

    expect(result).toEqual([usEast]);
  });

  it("matches part of a workspace name", () => {
    expect(filterRegions(regions, { ...noFilters, search: "market" }).map((r) => r.name)).toEqual(["US"]);
  });

  it("ignores surrounding whitespace in the search", () => {
    expect(filterRegions(regions, { ...noFilters, search: "  production  " }).map((r) => r.name)).toEqual([
      "us-east-1",
    ]);
  });

  it("filters by region type", () => {
    expect(filterRegions(regions, { ...noFilters, regionType: "airbyteManaged" }).map((r) => r.name)).toEqual(["US"]);
    expect(filterRegions(regions, { ...noFilters, regionType: "selfManaged" }).map((r) => r.name)).toEqual([
      "eu-west-1",
      "us-east-1",
    ]);
  });

  it("matches a health state when any data plane in the region has it", () => {
    expect(filterRegions(regions, { ...noFilters, health: "UNKNOWN" }).map((r) => r.name)).toEqual(["us-east-1"]);
    expect(filterRegions(regions, { ...noFilters, health: "DEGRADED" }).map((r) => r.name)).toEqual(["eu-west-1"]);
    expect(filterRegions(regions, { ...noFilters, health: "UNHEALTHY" })).toEqual([]);
  });

  it("excludes Airbyte-managed regions from any health filter", () => {
    const result = filterRegions(regions, { ...noFilters, regionType: "airbyteManaged", health: "HEALTHY" });

    expect(result).toEqual([]);
  });

  it("applies search and filters together", () => {
    const result = filterRegions(regions, { search: "us", regionType: "selfManaged", health: "HEALTHY" });

    expect(result.map((r) => r.name)).toEqual(["us-east-1"]);
  });
});
