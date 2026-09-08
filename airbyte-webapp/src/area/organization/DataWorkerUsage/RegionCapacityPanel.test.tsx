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
 * Opens a stepper's menu.
 *
 * Headless UI opens on the full pointer sequence, so this needs userEvent rather than a bare
 * fireEvent click. `delay: null` stops userEvent from advancing the fake clock between events,
 * which would otherwise trip the reallocation debounce before the test means to.
 */
const openStepper = async (testId: string) => {
  const user = userEvent.setup({ advanceTimers: jest.advanceTimersByTime, delay: null });
  await user.click(screen.getByTestId(testId));
  return user;
};

/** Opens a stepper's menu and picks the region on the other side of the reallocation. */
const pickRegion = async (testId: string, optionName: RegExp) => {
  const user = await openStepper(testId);
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
    expect(screen.getByTestId("region-capacity-row-us-east")).toHaveTextContent("Peak 3.01 / 3.0 DW");
    expect(screen.getByTestId("region-capacity-row-eu-west")).toHaveTextContent("Peak 1.41 / 2.0 DW");
    expect(screen.getByTestId("region-capacity-row-ap-south")).toHaveTextContent("Peak 0.00 / 0.0 DW");
  });

  it("leaves the contracted total untouched while a reallocation is staged", async () => {
    await renderPanel();

    await pickRegion("region-capacity-increment-eu-west", /US East/);

    // Reallocating only moves capacity between regions, so the organization's total never shifts.
    expect(screen.getByRole("heading", { name: "Region capacity" }).parentElement).toHaveTextContent("Contracted 5 DW");
  });

  it("raises the peak when the selected range widens to include an earlier spike", async () => {
    // The one-day window only sees the 3.01 sample.
    const { unmount } = await renderPanel();
    expect(screen.getByTestId("region-capacity-row-us-east")).toHaveTextContent("Peak 3.01 / 3.0 DW");
    unmount();

    // A month-wide window reaches the 6.5 sample five days earlier, so the peak rises with it.
    await renderPanel({
      requestDateRange: ["2026-07-25", "2026-08-25"],
      displayRange: ["2026-07-25T00:00:00.000Z", "2026-08-25T00:00:00.000Z"],
      selectedTimeRange: "1m",
    });
    expect(screen.getByTestId("region-capacity-row-us-east")).toHaveTextContent("Peak 6.50 / 3.0 DW");
  });

  it("selects a region when its row is clicked", async () => {
    await renderPanel();

    fireEvent.click(screen.getByTestId("region-capacity-row-eu-west"));

    expect(onSelectRegion).toHaveBeenCalledWith("eu-west");
  });

  it("sends nothing until the presses stop, then one call for the pair", async () => {
    await renderPanel();

    await pickRegion("region-capacity-increment-eu-west", /US East/);

    // Still staged: the row already shows the new number but no request has gone out.
    expect(screen.getByTestId("region-capacity-row-eu-west")).toHaveTextContent("Peak 1.41 / 3.0 DW");
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

    await pickRegion("region-capacity-increment-eu-west", /US East/);
    await pickRegion("region-capacity-increment-eu-west", /US East/);

    expect(screen.getByTestId("region-capacity-row-us-east")).toHaveTextContent("Peak 3.01 / 1.0 DW");
    expect(screen.getByTestId("region-capacity-row-eu-west")).toHaveTextContent("Peak 1.41 / 4.0 DW");

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

    await pickRegion("region-capacity-increment-eu-west", /US East/);
    await pickRegion("region-capacity-decrement-eu-west", /US East/);

    expect(screen.getByTestId("region-capacity-row-us-east")).toHaveTextContent("Peak 3.01 / 3.0 DW");
    expect(screen.getByTestId("region-capacity-row-eu-west")).toHaveTextContent("Peak 1.41 / 2.0 DW");

    await runDebounce();

    expect(mockReallocate).not.toHaveBeenCalled();
  });

  it("stops the batch and rolls every row back to the server's numbers when a call in the sequence fails", async () => {
    mockReallocate.mockImplementationOnce(() => Promise.reject(new Error("409 Conflict")));
    await renderPanel();

    await pickRegion("region-capacity-increment-eu-west", /US East/);
    await pickRegion("region-capacity-increment-ap-south", /EU West/);

    // Two distinct pairs are staged at once: us-east->eu-west and eu-west->ap-south.
    expect(screen.getByTestId("region-capacity-row-us-east")).toHaveTextContent("Peak 3.01 / 2.0 DW");
    expect(screen.getByTestId("region-capacity-row-ap-south")).toHaveTextContent("Peak 0.00 / 1.0 DW");

    await runDebounce();
    await act(async () => {});

    // The first pair's rejection stops the loop before the second pair's call ever goes out.
    expect(mockReallocate).toHaveBeenCalledTimes(1);

    // Every row falls back to what the server actually holds, and the steppers work again.
    expect(screen.getByTestId("region-capacity-row-us-east")).toHaveTextContent("Peak 3.01 / 3.0 DW");
    expect(screen.getByTestId("region-capacity-row-eu-west")).toHaveTextContent("Peak 1.41 / 2.0 DW");
    expect(screen.getByTestId("region-capacity-row-ap-south")).toHaveTextContent("Peak 0.00 / 0.0 DW");
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
    expect(screen.getByTestId("region-capacity-row-ap-south")).toHaveTextContent("Peak 0.00 / 0.0 DW");

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

  it("moves half a Data Worker when the 0.5 preset is picked", async () => {
    await renderPanel();

    const user = await openStepper("region-capacity-decrement-us-east");
    await user.click(screen.getByTestId("region-capacity-decrement-us-east-preset-0.5"));
    await user.click(await screen.findByRole("menuitem", { name: /EU West/ }));

    expect(screen.getByTestId("region-capacity-row-us-east")).toHaveTextContent("Peak 3.01 / 2.5 DW");
    expect(screen.getByTestId("region-capacity-row-eu-west")).toHaveTextContent("Peak 1.41 / 2.5 DW");

    await runDebounce();

    expect(mockReallocate).toHaveBeenCalledWith({
      fromDataplaneGroupId: "us-east",
      toDataplaneGroupId: "eu-west",
      amount: 0.5,
    });
  });

  it("walks the amount in halves and stops at the largest region it could come from", async () => {
    await renderPanel();

    // AP South holds nothing, so the most it can take in one move is US East's whole 3.
    const user = await openStepper("region-capacity-increment-ap-south");
    const increment = screen.getByTestId("region-capacity-increment-ap-south-amount-increment");
    await user.click(increment);
    await user.click(increment);
    await user.click(increment);
    await user.click(increment);

    expect(increment).toBeDisabled();
    await user.click(await screen.findByRole("menuitem", { name: /US East/ }));

    await runDebounce();

    expect(mockReallocate).toHaveBeenCalledWith({
      fromDataplaneGroupId: "us-east",
      toDataplaneGroupId: "ap-south",
      amount: 3,
    });
  });

  it("empties the region the move is drawn from when All is picked", async () => {
    await renderPanel();

    // "All" resolves against the region clicked, so taking All from EU West moves its 2 — not
    // US East's 3, which is the largest holding and so what the stepper caps at.
    const user = await openStepper("region-capacity-increment-ap-south");
    await user.click(screen.getByTestId("region-capacity-increment-ap-south-preset-all"));
    await user.click(await screen.findByRole("menuitem", { name: /EU West/ }));

    expect(screen.getByTestId("region-capacity-row-eu-west")).toHaveTextContent("Peak 1.41 / 0.0 DW");
    expect(screen.getByTestId("region-capacity-row-ap-south")).toHaveTextContent("Peak 0.00 / 2.0 DW");

    await runDebounce();

    expect(mockReallocate).toHaveBeenCalledWith({
      fromDataplaneGroupId: "eu-west",
      toDataplaneGroupId: "ap-south",
      amount: 2,
    });
  });

  it("disables presets above what can move and regions that cannot supply the amount", async () => {
    mockAllocations = {
      organization_id: "organization-1",
      total_allocated_capacity: 4,
      allocations: [
        { dataplane_group_id: "us-east", allocated_capacity: 3 },
        { dataplane_group_id: "eu-west", allocated_capacity: 1 },
      ],
    };
    await renderPanel();

    // EU West only holds 1, so it cannot give 2 away.
    const giving = await openStepper("region-capacity-decrement-eu-west");
    expect(screen.getByTestId("region-capacity-decrement-eu-west-preset-1")).toBeEnabled();
    expect(screen.getByTestId("region-capacity-decrement-eu-west-preset-2")).toBeDisabled();
    await giving.keyboard("{Escape}");

    // Asking for 2 leaves US East as the only region that can supply it.
    const taking = await openStepper("region-capacity-increment-ap-south");
    await taking.click(screen.getByTestId("region-capacity-increment-ap-south-preset-2"));
    expect(await screen.findByRole("menuitem", { name: /US East/ })).toBeEnabled();
    expect(screen.getByRole("menuitem", { name: /EU West/ })).toBeDisabled();
  });

  it("stages only what a move back cannot cancel", async () => {
    await renderPanel();

    // 2 out of US East, then 0.5 of it back, leaves 1.5 staged in the original direction.
    const giving = await openStepper("region-capacity-decrement-us-east");
    await giving.click(screen.getByTestId("region-capacity-decrement-us-east-preset-2"));
    await giving.click(await screen.findByRole("menuitem", { name: /EU West/ }));

    const taking = await openStepper("region-capacity-increment-us-east");
    await taking.click(screen.getByTestId("region-capacity-increment-us-east-preset-0.5"));
    await taking.click(await screen.findByRole("menuitem", { name: /EU West/ }));

    expect(screen.getByTestId("region-capacity-row-us-east")).toHaveTextContent("Peak 3.01 / 1.5 DW");
    expect(screen.getByTestId("region-capacity-row-eu-west")).toHaveTextContent("Peak 1.41 / 3.5 DW");

    await runDebounce();

    expect(mockReallocate).toHaveBeenCalledTimes(1);
    expect(mockReallocate).toHaveBeenCalledWith({
      fromDataplaneGroupId: "us-east",
      toDataplaneGroupId: "eu-west",
      amount: 1.5,
    });
  });
});
