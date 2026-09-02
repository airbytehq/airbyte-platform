import { fireEvent, screen, waitFor } from "@testing-library/react";

import { render } from "test-utils";
import { mockExperiments } from "test-utils/mockExperiments";

import { useOrganizationWorkerUsage } from "core/api";

import { DataWorkerUsage } from "./DataWorkerUsage";

interface MockUsageByWorkspaceGraphProps {
  selectedRegionId: string;
  requestDateRange: [string, string];
  displayRange: [string, string];
  historicalRequestDateRange: [string, string];
  historicalDisplayRange: [string, string];
  selectedTimeRange: "1d" | "1w" | "1m" | "1q" | "1y";
  comparisonEnabled: boolean;
}

interface MockRegionCapacityPanelProps {
  regions: Array<{ dataplane_group_id: string }>;
  selectedRegionId: string | null;
  requestDateRange: [string, string];
  displayRange: [string, string];
  selectedTimeRange: "1d" | "1w" | "1m" | "1q" | "1y";
}

const mockUsageByWorkspaceGraph = jest.fn();
const mockRegionCapacityPanel = jest.fn();
const defaultRegions = [
  {
    name: "US West",
    dataplane_group_id: "region-1",
    organization_id: "organization-1",
    enabled: true,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
    dataplanes: [],
  },
];
let mockRegions = defaultRegions;
let mockPendingOrganizationUsageRequest: Promise<void> | null = null;
const mockOrganizationUsage = {
  committedDataWorkers: 4,
  regions: [
    {
      id: "region-1",
      name: "US West",
      workspaces: [
        {
          id: "workspace-1",
          name: "Workspace 1",
          dataWorkers: [{ date: "2026-08-24T18:00:00Z", used: 1 }],
        },
      ],
    },
  ],
};

jest.mock("core/api", () => ({
  useListDataplaneGroups: jest.fn(() => mockRegions),
  useOrganizationWorkerUsage: jest.fn((params: { startDate: string }) => {
    if (params.startDate === "2025-08-25" && mockPendingOrganizationUsageRequest) {
      throw mockPendingOrganizationUsageRequest;
    }

    return mockOrganizationUsage;
  }),
}));

jest.mock("./UsageByWorkspaceGraph", () => {
  const React = jest.requireActual<typeof import("react")>("react");

  return {
    UsageByWorkspaceGraph: (props: MockUsageByWorkspaceGraphProps) => {
      mockUsageByWorkspaceGraph(props);
      return React.createElement("div", { "data-testid": "usage-by-workspace-graph" });
    },
  };
});

jest.mock("./RegionCapacityPanel", () => {
  const React = jest.requireActual<typeof import("react")>("react");

  return {
    RegionCapacityPanel: (props: MockRegionCapacityPanelProps) => {
      mockRegionCapacityPanel(props);
      return React.createElement("div", { "data-testid": "region-capacity-panel" });
    },
  };
});

const lastGraphProps = (): MockUsageByWorkspaceGraphProps =>
  mockUsageByWorkspaceGraph.mock.calls[mockUsageByWorkspaceGraph.mock.calls.length - 1][0];

const lastCapacityPanelProps = (): MockRegionCapacityPanelProps =>
  mockRegionCapacityPanel.mock.calls[mockRegionCapacityPanel.mock.calls.length - 1][0];

jest.useFakeTimers();

describe(`${DataWorkerUsage.name}`, () => {
  beforeEach(() => {
    jest.clearAllMocks();
    jest.setSystemTime(new Date("2026-08-24T12:34:00-07:00"));
    mockRegions = defaultRegions;
    mockPendingOrganizationUsageRequest = null;
    // Matches the production default, so the surrounding suite exercises the pre-flag page.
    mockExperiments({ "platform.enable-data-worker-allocation": false });
  });

  afterAll(() => {
    jest.useRealTimers();
  });

  it("defaults to 1W and uses its aligned hourly request and display bounds", async () => {
    await render(<DataWorkerUsage />);

    await waitFor(() => expect(screen.getByTestId("usage-by-workspace-graph")).toBeInTheDocument());

    const heading = screen.getByRole("heading", { name: "Peak data worker usage" });
    const description = screen.getByText("How much of your contracted capacity each region is using.");
    const timeRangeControl = screen.getByRole("group", { name: "Usage time range" });
    const regionControl = screen.getByRole("button", { name: "US West" });
    const comparisonControl = screen.getByRole("checkbox", { name: "Compare to previous period" });
    const comparisonLabel = screen.getByText("Compare to previous period");
    const graph = screen.getByTestId("usage-by-workspace-graph");

    expect(heading.compareDocumentPosition(description) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(description.compareDocumentPosition(timeRangeControl) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(timeRangeControl.parentElement).toContainElement(regionControl);
    expect(timeRangeControl.parentElement?.firstElementChild).toBe(timeRangeControl);
    expect(comparisonLabel.compareDocumentPosition(comparisonControl) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(comparisonControl).not.toBeChecked();
    expect(regionControl.querySelector('[data-icon="globe"]')).toBeInTheDocument();
    expect(regionControl.compareDocumentPosition(graph) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();

    expect(screen.getByRole("radio", { name: "1D" })).not.toBeChecked();
    expect(screen.getByRole("radio", { name: "1W" })).toBeChecked();
    expect(screen.getByRole("radio", { name: "1M" })).not.toBeChecked();
    expect(screen.getByRole("radio", { name: "1Q" })).not.toBeChecked();
    expect(screen.getByRole("radio", { name: "1Y" })).not.toBeChecked();
    expect(screen.queryByTestId("range-date-picker")).not.toBeInTheDocument();
    expect(useOrganizationWorkerUsage).toHaveBeenLastCalledWith(
      {
        startDate: "2026-08-17",
        endDate: "2026-08-24",
      },
      60_000
    );

    expect(lastGraphProps()).toEqual(
      expect.objectContaining({
        selectedRegionId: "region-1",
        selectedTimeRange: "1w",
        requestDateRange: ["2026-08-17", "2026-08-24"],
        displayRange: ["2026-08-17T20:00:00.000Z", "2026-08-24T20:00:00.000Z"],
        historicalRequestDateRange: ["2026-08-10", "2026-08-17"],
        historicalDisplayRange: ["2026-08-10T20:00:00.000Z", "2026-08-17T20:00:00.000Z"],
        comparisonEnabled: false,
      })
    );

    fireEvent.click(regionControl);
    const regionOption = await screen.findByRole("option", { name: "US West" });
    expect(regionOption.querySelector('[data-icon="globe"]')).not.toBeInTheDocument();
  });

  it("switches to aligned 1D and 1M windows without persisting the choice in the URL", async () => {
    await render(<DataWorkerUsage />);
    await waitFor(() => expect(screen.getByTestId("usage-by-workspace-graph")).toBeInTheDocument());

    fireEvent.click(screen.getByRole("radio", { name: "1D" }));

    expect(screen.getByRole("radio", { name: "1D" })).toBeChecked();
    expect(useOrganizationWorkerUsage).toHaveBeenLastCalledWith(
      {
        startDate: "2026-08-23",
        endDate: "2026-08-24",
      },
      60_000
    );
    expect(lastGraphProps()).toEqual(
      expect.objectContaining({
        selectedTimeRange: "1d",
        requestDateRange: ["2026-08-23", "2026-08-24"],
        displayRange: ["2026-08-23T20:00:00.000Z", "2026-08-24T20:00:00.000Z"],
        historicalRequestDateRange: ["2026-08-22", "2026-08-23"],
        historicalDisplayRange: ["2026-08-22T20:00:00.000Z", "2026-08-23T20:00:00.000Z"],
      })
    );

    fireEvent.click(screen.getByRole("radio", { name: "1M" }));

    expect(screen.getByRole("radio", { name: "1M" })).toBeChecked();
    expect(useOrganizationWorkerUsage).toHaveBeenLastCalledWith(
      {
        startDate: "2026-07-25",
        endDate: "2026-08-25",
      },
      60_000
    );
    expect(lastGraphProps()).toEqual(
      expect.objectContaining({
        selectedTimeRange: "1m",
        requestDateRange: ["2026-07-25", "2026-08-25"],
        displayRange: ["2026-07-25T07:00:00.000Z", "2026-08-25T07:00:00.000Z"],
        historicalRequestDateRange: ["2026-06-25", "2026-07-25"],
        historicalDisplayRange: ["2026-06-25T07:00:00.000Z", "2026-07-25T07:00:00.000Z"],
      })
    );
    expect(window.location.search).toBe("");
  });

  it("uses calendar quarter and year windows with five-minute polling", async () => {
    await render(<DataWorkerUsage />);
    await waitFor(() => expect(screen.getByTestId("usage-by-workspace-graph")).toBeInTheDocument());

    fireEvent.click(screen.getByRole("radio", { name: "1Q" }));

    expect(useOrganizationWorkerUsage).toHaveBeenLastCalledWith(
      { startDate: "2026-05-25", endDate: "2026-08-25" },
      300_000
    );
    expect(lastGraphProps()).toEqual(
      expect.objectContaining({
        selectedTimeRange: "1q",
        requestDateRange: ["2026-05-25", "2026-08-25"],
        displayRange: ["2026-05-25T07:00:00.000Z", "2026-08-25T07:00:00.000Z"],
        historicalRequestDateRange: ["2026-02-25", "2026-05-25"],
        historicalDisplayRange: ["2026-02-25T08:00:00.000Z", "2026-05-25T07:00:00.000Z"],
      })
    );

    fireEvent.click(screen.getByRole("radio", { name: "1Y" }));

    expect(useOrganizationWorkerUsage).toHaveBeenLastCalledWith(
      { startDate: "2025-08-25", endDate: "2026-08-25" },
      300_000
    );
    expect(lastGraphProps()).toEqual(
      expect.objectContaining({
        selectedTimeRange: "1y",
        requestDateRange: ["2025-08-25", "2026-08-25"],
        displayRange: ["2025-08-25T07:00:00.000Z", "2026-08-25T07:00:00.000Z"],
        historicalRequestDateRange: ["2024-08-25", "2025-08-25"],
        historicalDisplayRange: ["2024-08-25T07:00:00.000Z", "2025-08-25T07:00:00.000Z"],
      })
    );
  });

  it("uses calendar-year arithmetic when the rolling range ends on leap day", async () => {
    jest.setSystemTime(new Date("2024-02-28T12:34:00-08:00"));

    await render(<DataWorkerUsage />);
    await waitFor(() => expect(screen.getByTestId("usage-by-workspace-graph")).toBeInTheDocument());

    fireEvent.click(screen.getByRole("radio", { name: "1Y" }));

    expect(useOrganizationWorkerUsage).toHaveBeenLastCalledWith(
      { startDate: "2023-02-28", endDate: "2024-02-29" },
      300_000
    );
    expect(lastGraphProps()).toEqual(
      expect.objectContaining({
        displayRange: ["2023-02-28T08:00:00.000Z", "2024-02-29T08:00:00.000Z"],
        historicalRequestDateRange: ["2022-02-28", "2023-02-28"],
        historicalDisplayRange: ["2022-02-28T08:00:00.000Z", "2023-02-28T08:00:00.000Z"],
      })
    );
  });

  it("enables comparison through its label and keeps it enabled across range changes", async () => {
    await render(<DataWorkerUsage />);
    await waitFor(() => expect(screen.getByTestId("usage-by-workspace-graph")).toBeInTheDocument());

    fireEvent.click(screen.getByText("Compare to previous period"));

    expect(screen.getByRole("checkbox", { name: "Compare to previous period" })).toBeChecked();
    expect(lastGraphProps().comparisonEnabled).toBe(true);

    fireEvent.click(screen.getByRole("radio", { name: "1M" }));

    expect(screen.getByRole("checkbox", { name: "Compare to previous period" })).toBeChecked();
    expect(lastGraphProps()).toEqual(
      expect.objectContaining({
        selectedTimeRange: "1m",
        comparisonEnabled: true,
        historicalRequestDateRange: ["2026-06-25", "2026-07-25"],
      })
    );
  });

  it("uses the previous calendar month when adjacent periods have unequal lengths", async () => {
    jest.setSystemTime(new Date("2026-03-28T12:34:00-07:00"));

    await render(<DataWorkerUsage />);
    await waitFor(() => expect(screen.getByTestId("usage-by-workspace-graph")).toBeInTheDocument());
    fireEvent.click(screen.getByRole("radio", { name: "1M" }));

    expect(lastGraphProps()).toEqual(
      expect.objectContaining({
        requestDateRange: ["2026-02-28", "2026-03-29"],
        displayRange: ["2026-02-28T08:00:00.000Z", "2026-03-29T07:00:00.000Z"],
        historicalRequestDateRange: ["2026-01-28", "2026-02-28"],
        historicalDisplayRange: ["2026-01-28T08:00:00.000Z", "2026-02-28T08:00:00.000Z"],
      })
    );
  });

  it("keeps both weekly windows at exactly 168 hours next to a DST boundary", async () => {
    jest.setSystemTime(new Date("2026-03-08T01:34:00-08:00"));

    await render(<DataWorkerUsage />);
    await waitFor(() => expect(screen.getByTestId("usage-by-workspace-graph")).toBeInTheDocument());

    expect(lastGraphProps()).toEqual(
      expect.objectContaining({
        requestDateRange: ["2026-03-01", "2026-03-08"],
        displayRange: ["2026-03-01T10:00:00.000Z", "2026-03-08T10:00:00.000Z"],
        historicalRequestDateRange: ["2026-02-22", "2026-03-01"],
        historicalDisplayRange: ["2026-02-22T10:00:00.000Z", "2026-03-01T10:00:00.000Z"],
      })
    );
  });

  it("shows the selected range and chart loader while a new range query is pending", async () => {
    await render(<DataWorkerUsage />);
    await waitFor(() => expect(screen.getByTestId("usage-by-workspace-graph")).toBeInTheDocument());

    let resolvePendingOrganizationUsageRequest!: () => void;
    mockPendingOrganizationUsageRequest = new Promise<void>((resolve) => {
      resolvePendingOrganizationUsageRequest = resolve;
    });

    fireEvent.click(screen.getByRole("radio", { name: "1Y" }));

    expect(screen.getByRole("radio", { name: "1Y" })).toBeChecked();
    expect(screen.getByText("Loading usage data...")).toBeInTheDocument();
    expect(screen.queryByTestId("usage-by-workspace-graph")).not.toBeInTheDocument();
    await waitFor(() =>
      expect(useOrganizationWorkerUsage).toHaveBeenLastCalledWith(
        { startDate: "2025-08-25", endDate: "2026-08-25" },
        300_000
      )
    );

    mockPendingOrganizationUsageRequest = null;
    resolvePendingOrganizationUsageRequest();

    await waitFor(() => expect(screen.getByTestId("usage-by-workspace-graph")).toBeInTheDocument());
    expect(lastGraphProps()).toEqual(expect.objectContaining({ selectedTimeRange: "1y" }));
  });

  it("renders the region placeholder and no graph when there are no regions", async () => {
    mockRegions = [];

    await render(<DataWorkerUsage />);

    const regionControl = screen.getByRole("button");
    expect(regionControl.querySelector('[data-icon="globe"]')).not.toBeInTheDocument();
    expect(regionControl).not.toHaveTextContent("US West");
    expect(screen.queryByTestId("usage-by-workspace-graph")).not.toBeInTheDocument();
  });

  it("hides the capacity panel while the allocation flag is off", async () => {
    await render(<DataWorkerUsage />);

    await waitFor(() => expect(screen.getByTestId("usage-by-workspace-graph")).toBeInTheDocument());

    // Never rendering the panel is also what keeps the admin-only allocation request from firing.
    expect(screen.queryByTestId("region-capacity-panel")).not.toBeInTheDocument();
    expect(mockRegionCapacityPanel).not.toHaveBeenCalled();
  });

  it("adds the capacity panel below the chart when the allocation flag is on", async () => {
    mockExperiments({ "platform.enable-data-worker-allocation": true });

    await render(<DataWorkerUsage />);

    await waitFor(() => expect(screen.getByTestId("usage-by-workspace-graph")).toBeInTheDocument());

    const capacityPanel = screen.getByTestId("region-capacity-panel");
    const graph = screen.getByTestId("usage-by-workspace-graph");

    expect(screen.getByRole("button", { name: "US West" })).toBeInTheDocument();
    expect(graph.compareDocumentPosition(capacityPanel) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();

    // The panel owns region selection, so it has to receive the window currently on screen.
    expect(lastCapacityPanelProps()).toEqual(
      expect.objectContaining({
        regions: mockRegions,
        selectedRegionId: "region-1",
        selectedTimeRange: "1w",
        requestDateRange: ["2026-08-17", "2026-08-24"],
        displayRange: ["2026-08-17T20:00:00.000Z", "2026-08-24T20:00:00.000Z"],
      })
    );
  });
});
