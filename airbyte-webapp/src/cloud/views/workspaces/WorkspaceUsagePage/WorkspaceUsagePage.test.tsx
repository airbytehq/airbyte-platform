import { screen } from "@testing-library/react";

import { mocked, render } from "test-utils";

import { useCurrentWorkspace, useGetDataplaneGroup } from "core/api";
import { useExperiment } from "core/services/Experiment";
import { FeatureItem } from "core/services/features";

import { WorkspaceUsagePage } from "./WorkspaceUsagePage";

jest.mock("area/organization/components/SetupBillingAlertsLink", () => ({
  SetupBillingAlertsLink: () => null,
}));

jest.mock("cloud/area/billing/components/UsagePerDayGraph", () => ({
  UsagePerDayGraph: () => null,
}));

jest.mock("core/api", () => ({
  useCurrentWorkspace: jest.fn(),
  useGetDataplaneGroup: jest.fn(),
}));

jest.mock("core/services/analytics", () => ({
  PageTrackingCodes: { SETTINGS_WORKSPACE_USAGE: "settings.workspace.usage" },
  useTrackPage: jest.fn(),
}));

jest.mock("core/services/Experiment", () => ({
  ...jest.requireActual("core/services/Experiment"),
  useExperiment: jest.fn(),
}));

jest.mock("./components/CreditsUsageContext", () => ({
  useCreditsContext: jest.fn(() => ({
    freeAndPaidUsageByTimeChunk: [],
    hasFreeUsage: false,
    hasInternalUsage: false,
  })),
  WorkspaceCreditUsageContextProvider: ({ children }: React.PropsWithChildren) => children,
}));

jest.mock("./components/CreditsUsageFilters", () => ({
  CreditsUsageFilters: () => null,
}));

jest.mock("./components/UsagePerConnectionTable", () => ({
  UsagePerConnectionTable: () => null,
}));

jest.mock("./components/WorkspaceDataWorkerUsageGraph", () => ({
  WorkspaceDataWorkerUsageGraph: () => null,
}));

describe(`${WorkspaceUsagePage.name}`, () => {
  const getDataplaneGroup = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
    mocked(useCurrentWorkspace).mockReturnValue({
      dataplaneGroupId: "dataplane-group-1",
    } as ReturnType<typeof useCurrentWorkspace>);
    mocked(useGetDataplaneGroup).mockReturnValue({ getDataplaneGroup });
    mocked(useExperiment).mockReturnValue(true);
  });

  it("renders the current workspace region below the page heading", async () => {
    getDataplaneGroup.mockReturnValue({ name: "US East (N. Virginia)" });

    await render(<WorkspaceUsagePage />, undefined, [FeatureItem.AllowDataWorkerCapacity]);

    const heading = screen.getByRole("heading", { name: "Workspace usage" });
    const region = screen.getByText("Region: US East (N. Virginia)");
    const description = screen.getByText("Data worker usage for this workspace.");

    expect(getDataplaneGroup).toHaveBeenCalledWith("dataplane-group-1");
    expect(region.parentElement?.querySelector('[data-icon="globe"]')).toBeInTheDocument();
    expect(heading.compareDocumentPosition(region)).toBe(Node.DOCUMENT_POSITION_FOLLOWING);
    expect(region.compareDocumentPosition(description)).toBe(Node.DOCUMENT_POSITION_FOLLOWING);
  });

  it("omits the region row when the workspace assignment cannot be resolved", async () => {
    getDataplaneGroup.mockReturnValue(undefined);

    await render(<WorkspaceUsagePage />, undefined, [FeatureItem.AllowDataWorkerCapacity]);

    expect(getDataplaneGroup).toHaveBeenCalledWith("dataplane-group-1");
    expect(screen.queryByText(/^Region:/)).not.toBeInTheDocument();
    expect(document.querySelector('[data-icon="globe"]')).not.toBeInTheDocument();
  });

  it("does not load dataplane groups when the workspace has no region assignment", async () => {
    mocked(useCurrentWorkspace).mockReturnValue({
      dataplaneGroupId: undefined,
    } as ReturnType<typeof useCurrentWorkspace>);

    await render(<WorkspaceUsagePage />, undefined, [FeatureItem.AllowDataWorkerCapacity]);

    expect(useGetDataplaneGroup).not.toHaveBeenCalled();
    expect(screen.queryByText(/^Region:/)).not.toBeInTheDocument();
  });

  it("does not load or render the region for the credits-based usage view", async () => {
    mocked(useExperiment).mockReturnValue(false);

    await render(<WorkspaceUsagePage />, undefined, [FeatureItem.AllowDataWorkerCapacity]);

    expect(useGetDataplaneGroup).not.toHaveBeenCalled();
    expect(screen.queryByText(/^Region:/)).not.toBeInTheDocument();
  });

  it("does not load or render the region without the data worker entitlement", async () => {
    getDataplaneGroup.mockReturnValue({ name: "US East (N. Virginia)" });

    await render(<WorkspaceUsagePage />, undefined, []);

    expect(useGetDataplaneGroup).not.toHaveBeenCalled();
    expect(screen.queryByText(/^Region:/)).not.toBeInTheDocument();
    expect(screen.getByRole("link", { name: "credits" })).toBeInTheDocument();
  });
});
