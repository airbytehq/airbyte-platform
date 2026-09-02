import { act, fireEvent, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { render } from "test-utils";

import { RegionCapacityPanel } from "./RegionCapacityPanel";

const REALLOCATE_DEBOUNCE_MS = 1000;

const buildRegion = (id: string, name: string) => ({
  name,
  dataplane_group_id: id,
  organization_id: "organization-1",
  enabled: true,
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
  dataplanes: [],
});

// "ap-south" deliberately has no allocation row — the endpoint omits regions the organization holds
// nothing in rather than returning them as zero.
const regions = [
  buildRegion("us-east", "US East"),
  buildRegion("eu-west", "EU West"),
  buildRegion("ap-south", "AP South"),
];

const mockReallocate = jest.fn(() => Promise.resolve());
let mockAllocations = {
  organization_id: "organization-1",
  total_allocated_capacity: 5,
  allocations: [
    { dataplane_group_id: "us-east", allocated_capacity: 3 },
    { dataplane_group_id: "eu-west", allocated_capacity: 2 },
  ],
};

const mockUsage = {
  committedDataWorkers: 8,
  regions: [
    {
      id: "us-east",
      name: "US East",
      workspaces: [
        {
          id: "workspace-1",
          name: "Workspace 1",
          dataWorkers: [
            { date: "2026-08-24T18:00:00Z", used: 3.01 },
            // Outside the single-day window the other tests use, so only a wider range finds it.
            { date: "2026-08-19T18:00:00Z", used: 6.5 },
          ],
        },
      ],
    },
    {
      id: "eu-west",
      name: "EU West",
      workspaces: [
        { id: "workspace-2", name: "Workspace 2", dataWorkers: [{ date: "2026-08-24T18:00:00Z", used: 1.41 }] },
      ],
    },
  ],
};

jest.mock("core/api", () => ({
  useListDataWorkerAllocations: () => mockAllocations,
  useOrganizationWorkerUsage: () => mockUsage,
  useReallocateDataWorkerCapacity: () => ({ mutateAsync: mockReallocate }),
}));

const onSelectRegion = jest.fn();

const renderPanel = (overrides: Partial<React.ComponentProps<typeof RegionCapacityPanel>> = {}) =>
  render(
    <RegionCapacityPanel
      regions={regions}
      selectedRegionId="us-east"
      onSelectRegion={onSelectRegion}
      requestDateRange={["2026-08-24", "2026-08-25"]}
      displayRange={["2026-08-24T00:00:00.000Z", "2026-08-25T00:00:00.000Z"]}
      selectedTimeRange="1w"
      {...overrides}
    />
  );

/**
 * Opens a stepper's menu and picks the region on the other side of the reallocation.
 *
 * Headless UI opens on the full pointer sequence, so this needs userEvent rather than a bare
 * fireEvent click. `delay: null` stops userEvent from advancing the fake clock between events,
 * which would otherwise trip the reallocation debounce before the test means to.
 */
const pickRegion = async (testId: string, optionName: string) => {
  const user = userEvent.setup({ advanceTimers: jest.advanceTimersByTime, delay: null });
  await user.click(screen.getByTestId(testId));
  await user.click(await screen.findByRole("menuitem", { name: optionName }));
};

const runDebounce = async () => {
  await act(async () => {
    jest.advanceTimersByTime(REALLOCATE_DEBOUNCE_MS);
  });
};

jest.useFakeTimers();

describe(`${RegionCapacityPanel.name}`, () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockAllocations = {
      organization_id: "organization-1",
      total_allocated_capacity: 5,
      allocations: [
        { dataplane_group_id: "us-east", allocated_capacity: 3 },
        { dataplane_group_id: "eu-west", allocated_capacity: 2 },
      ],
    };
  });

  afterAll(() => {
    jest.useRealTimers();
  });

  it("renders the contracted total and a row per region, defaulting an absent allocation to zero", async () => {
    await renderPanel();

    // Contracted comes from the allocation total, not an entitlement — capacity is always fully
    // allocated, so what the table sums to is what the organization contracted for.
    expect(screen.getByRole("heading", { name: "Region capacity" }).parentElement).toHaveTextContent("Contracted 5 DW");
    expect(screen.getByTestId("region-capacity-row-us-east")).toHaveTextContent("Peak 3.01 / 3 DW");
    expect(screen.getByTestId("region-capacity-row-eu-west")).toHaveTextContent("Peak 1.41 / 2 DW");
    expect(screen.getByTestId("region-capacity-row-ap-south")).toHaveTextContent("Peak 0.00 / 0 DW");
  });

  it("leaves the contracted total untouched while a reallocation is staged", async () => {
    await renderPanel();

    await pickRegion("region-capacity-increment-eu-west", "US East (3 DW)");

    // Reallocating only moves capacity between regions, so the organization's total never shifts.
    expect(screen.getByRole("heading", { name: "Region capacity" }).parentElement).toHaveTextContent("Contracted 5 DW");
  });

  it("raises the peak when the selected range widens to include an earlier spike", async () => {
    // The one-day window only sees the 3.01 sample.
    const { unmount } = await renderPanel();
    expect(screen.getByTestId("region-capacity-row-us-east")).toHaveTextContent("Peak 3.01 / 3 DW");
    unmount();

    // A month-wide window reaches the 6.5 sample five days earlier, so the peak rises with it.
    await renderPanel({
      requestDateRange: ["2026-07-25", "2026-08-25"],
      displayRange: ["2026-07-25T00:00:00.000Z", "2026-08-25T00:00:00.000Z"],
      selectedTimeRange: "1m",
    });
    expect(screen.getByTestId("region-capacity-row-us-east")).toHaveTextContent("Peak 6.50 / 3 DW");
  });

  it("selects a region when its row is clicked", async () => {
    await renderPanel();

    fireEvent.click(screen.getByTestId("region-capacity-row-eu-west"));

    expect(onSelectRegion).toHaveBeenCalledWith("eu-west");
  });

  it("sends nothing until the presses stop, then one call for the pair", async () => {
    await renderPanel();

    await pickRegion("region-capacity-increment-eu-west", "US East (3 DW)");

    // Still staged: the row already shows the new number but no request has gone out.
    expect(screen.getByTestId("region-capacity-row-eu-west")).toHaveTextContent("Peak 1.41 / 3 DW");
    expect(mockReallocate).not.toHaveBeenCalled();

    await runDebounce();

    expect(mockReallocate).toHaveBeenCalledTimes(1);
    expect(mockReallocate).toHaveBeenCalledWith({
      fromDataplaneGroupId: "us-east",
      toDataplaneGroupId: "eu-west",
      amount: 1,
    });
  });

  it("sums repeated presses on the same pair into a single call", async () => {
    await renderPanel();

    await pickRegion("region-capacity-increment-eu-west", "US East (3 DW)");
    await pickRegion("region-capacity-increment-eu-west", "US East (2 DW)");

    expect(screen.getByTestId("region-capacity-row-us-east")).toHaveTextContent("Peak 3.01 / 1 DW");
    expect(screen.getByTestId("region-capacity-row-eu-west")).toHaveTextContent("Peak 1.41 / 4 DW");

    await runDebounce();

    expect(mockReallocate).toHaveBeenCalledTimes(1);
    expect(mockReallocate).toHaveBeenCalledWith({
      fromDataplaneGroupId: "us-east",
      toDataplaneGroupId: "eu-west",
      amount: 2,
    });
  });

  it("cancels a staged reallocation when the opposite press undoes it", async () => {
    await renderPanel();

    await pickRegion("region-capacity-increment-eu-west", "US East (3 DW)");
    await pickRegion("region-capacity-decrement-eu-west", "US East (2 DW)");

    expect(screen.getByTestId("region-capacity-row-us-east")).toHaveTextContent("Peak 3.01 / 3 DW");
    expect(screen.getByTestId("region-capacity-row-eu-west")).toHaveTextContent("Peak 1.41 / 2 DW");

    await runDebounce();

    expect(mockReallocate).not.toHaveBeenCalled();
  });

  it("stops the batch and rolls every row back to the server's numbers when a call in the sequence fails", async () => {
    mockReallocate.mockImplementationOnce(() => Promise.reject(new Error("409 Conflict")));
    await renderPanel();

    await pickRegion("region-capacity-increment-eu-west", "US East (3 DW)");
    await pickRegion("region-capacity-increment-ap-south", "EU West (3 DW)");

    // Two distinct pairs are staged at once: us-east->eu-west and eu-west->ap-south.
    expect(screen.getByTestId("region-capacity-row-us-east")).toHaveTextContent("Peak 3.01 / 2 DW");
    expect(screen.getByTestId("region-capacity-row-ap-south")).toHaveTextContent("Peak 0.00 / 1 DW");

    await runDebounce();
    await act(async () => {});

    // The first pair's rejection stops the loop before the second pair's call ever goes out.
    expect(mockReallocate).toHaveBeenCalledTimes(1);

    // Every row falls back to what the server actually holds, and the steppers work again.
    expect(screen.getByTestId("region-capacity-row-us-east")).toHaveTextContent("Peak 3.01 / 3 DW");
    expect(screen.getByTestId("region-capacity-row-eu-west")).toHaveTextContent("Peak 1.41 / 2 DW");
    expect(screen.getByTestId("region-capacity-row-ap-south")).toHaveTextContent("Peak 0.00 / 0 DW");
    expect(screen.getByTestId("region-capacity-increment-eu-west")).toBeEnabled();
  });

  it("cannot take capacity out of a region that holds none", async () => {
    await renderPanel();

    expect(screen.getByTestId("region-capacity-decrement-ap-south")).toBeDisabled();
    expect(screen.getByTestId("region-capacity-decrement-us-east")).toBeEnabled();
  });

  it("opens no menu and stages nothing when the disabled decrement is clicked", async () => {
    await renderPanel();

    const user = userEvent.setup({ advanceTimers: jest.advanceTimersByTime, delay: null });
    await user.click(screen.getByTestId("region-capacity-decrement-ap-south"));

    // A disabled button nested in Headless UI's trigger used to leave the trigger live, so the menu
    // opened for a region holding nothing and the row rendered -1 before the server refused it.
    expect(screen.queryByRole("menu")).not.toBeInTheDocument();
    expect(screen.getByTestId("region-capacity-row-ap-south")).toHaveTextContent("Peak 0.00 / 0 DW");

    await runDebounce();

    expect(mockReallocate).not.toHaveBeenCalled();
  });

  it("cannot add capacity when no other region has any to give", async () => {
    mockAllocations = {
      organization_id: "organization-1",
      total_allocated_capacity: 3,
      allocations: [{ dataplane_group_id: "us-east", allocated_capacity: 3 }],
    };

    await renderPanel();

    // Every other region is empty, so US East has nowhere to draw from.
    expect(screen.getByTestId("region-capacity-increment-us-east")).toBeDisabled();
    expect(screen.getByTestId("region-capacity-increment-eu-west")).toBeEnabled();
  });
});
