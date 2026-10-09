import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { TestWrapper } from "test-utils";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";

import { useExperiment } from "core/services/Experiment";
import { FeatureItem } from "core/services/features";
import { links } from "core/utils/links";
import { useGeneratedIntent } from "core/utils/rbac";

import { CapacityReachedMessage } from "./CapacityReachedMessage";

const mockSetDismissedByWorkspace = jest.fn();
let mockDismissedByWorkspace: Record<string, boolean> = {};
let mockStatusCounts: { queued: number } | undefined;

jest.mock("area/organization/utils", () => ({
  ...jest.requireActual("area/organization/utils"),
  useCurrentOrganizationId: () => "org-id",
}));

jest.mock("core/services/Experiment", () => ({
  ...jest.requireActual("core/services/Experiment"),
  useExperiment: jest.fn(),
}));

jest.mock("core/utils/rbac", () => ({
  ...jest.requireActual("core/utils/rbac"),
  useGeneratedIntent: jest.fn(),
}));

jest.mock("core/api", () => ({
  useCurrentWorkspace: () => mockWorkspace,
  useGetConnectionStatusesCounts: () => ({ data: mockStatusCounts }),
}));

jest.mock("core/utils/useLocalStorage", () => ({
  useLocalStorage: () => [mockDismissedByWorkspace, mockSetDismissedByWorkspace],
}));

describe("CapacityReachedMessage", () => {
  beforeEach(() => {
    mockDismissedByWorkspace = {};
    mockStatusCounts = { queued: 0 };
    mockSetDismissedByWorkspace.mockClear();
    jest.mocked(useExperiment).mockReturnValue(false);
    jest.mocked(useGeneratedIntent).mockReturnValue(true);
  });

  const renderComponent = (features?: FeatureItem[]) => {
    return render(
      <TestWrapper features={features}>
        <CapacityReachedMessage />
      </TestWrapper>
    );
  };

  it("shows banner when queuedCount > 0 and not dismissed", () => {
    mockStatusCounts = { queued: 5 };
    mockDismissedByWorkspace = {};

    renderComponent();

    expect(screen.getByTestId("capacity-reached-banner")).toBeInTheDocument();
  });

  it("does not show banner when queuedCount is 0", () => {
    mockStatusCounts = { queued: 0 };
    mockDismissedByWorkspace = {};

    renderComponent();

    expect(screen.queryByTestId("capacity-reached-banner")).not.toBeInTheDocument();
  });

  it("does not show banner when dismissed for current workspace", () => {
    mockStatusCounts = { queued: 5 };
    mockDismissedByWorkspace = { [mockWorkspace.workspaceId]: true };

    renderComponent();

    expect(screen.queryByTestId("capacity-reached-banner")).not.toBeInTheDocument();
  });

  it("shows banner when dismissed for different workspace but not current", () => {
    mockStatusCounts = { queued: 5 };
    mockDismissedByWorkspace = { "different-workspace-id": true };

    renderComponent();

    expect(screen.getByTestId("capacity-reached-banner")).toBeInTheDocument();
  });

  it("calls setDismissedByWorkspace when close button is clicked", async () => {
    mockStatusCounts = { queued: 5 };
    mockDismissedByWorkspace = {};

    renderComponent();

    const closeButton = screen.getByRole("button");
    await userEvent.click(closeButton);

    expect(mockSetDismissedByWorkspace).toHaveBeenCalledWith(expect.any(Function));

    // Verify the updater function sets the correct workspace
    const updaterFn = mockSetDismissedByWorkspace.mock.calls[0][0];
    const result = updaterFn({});
    expect(result).toEqual({ [mockWorkspace.workspaceId]: true });
  });

  it("resets dismissed state when queuedCount becomes 0 and was previously dismissed", () => {
    mockStatusCounts = { queued: 0 };
    mockDismissedByWorkspace = { [mockWorkspace.workspaceId]: true };

    renderComponent();

    expect(mockSetDismissedByWorkspace).toHaveBeenCalledWith(expect.any(Function));

    // Verify the updater function resets to false
    const updaterFn = mockSetDismissedByWorkspace.mock.calls[0][0];
    const result = updaterFn({ [mockWorkspace.workspaceId]: true });
    expect(result).toEqual({ [mockWorkspace.workspaceId]: false });
  });

  it("does not reset dismissed state when data is not loaded yet", () => {
    mockStatusCounts = undefined; // Data not loaded
    mockDismissedByWorkspace = { [mockWorkspace.workspaceId]: true };

    renderComponent();

    // Should not call setDismissedByWorkspace because data isn't loaded
    expect(mockSetDismissedByWorkspace).not.toHaveBeenCalled();
  });

  it("does not reset dismissed state when queuedCount is 0 but was not dismissed", () => {
    mockStatusCounts = { queued: 0 };
    mockDismissedByWorkspace = { [mockWorkspace.workspaceId]: false };

    renderComponent();

    expect(mockSetDismissedByWorkspace).not.toHaveBeenCalled();
  });

  it("links admins to region allocation and on-demand capacity when both are enabled", () => {
    mockStatusCounts = { queued: 5 };
    jest.mocked(useExperiment).mockReturnValue(true);

    renderComponent([FeatureItem.AllowDataWorkerCapacity, FeatureItem.OnDemandCapacity]);

    expect(screen.getByTestId("capacity-reached-banner")).toHaveTextContent(
      "Maximum capacity reached. Additional syncs will be queued until capacity is available."
    );
    expect(screen.getByRole("link", { name: "allocate more capacity" })).toHaveAttribute(
      "href",
      "/organization/org-id/settings/organization-usage"
    );
    expect(screen.getByRole("link", { name: "use on-demand capacity" })).toHaveAttribute(
      "href",
      links.dataWorkerOnDemandCapacity
    );
  });

  it("links admins to region allocation when on-demand capacity is disabled", () => {
    mockStatusCounts = { queued: 5 };
    jest.mocked(useExperiment).mockReturnValue(true);

    renderComponent([FeatureItem.AllowDataWorkerCapacity]);

    expect(screen.getByRole("link", { name: "allocate more capacity" })).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "use on-demand capacity" })).not.toBeInTheDocument();
  });

  it("asks non-admins to contact an admin and links to on-demand capacity", () => {
    mockStatusCounts = { queued: 5 };
    jest.mocked(useExperiment).mockReturnValue(true);
    jest.mocked(useGeneratedIntent).mockReturnValue(false);

    renderComponent([FeatureItem.AllowDataWorkerCapacity, FeatureItem.OnDemandCapacity]);

    expect(screen.getByTestId("capacity-reached-banner")).toHaveTextContent(
      "Ask an organization admin to allocate more capacity to this region"
    );
    expect(screen.queryByRole("link", { name: "allocate more capacity" })).not.toBeInTheDocument();
    expect(screen.getByRole("link", { name: "use on-demand capacity" })).toHaveAttribute(
      "href",
      links.dataWorkerOnDemandCapacity
    );
  });

  it("asks non-admins to contact an admin when on-demand capacity is disabled", () => {
    mockStatusCounts = { queued: 5 };
    jest.mocked(useExperiment).mockReturnValue(true);
    jest.mocked(useGeneratedIntent).mockReturnValue(false);

    renderComponent([FeatureItem.AllowDataWorkerCapacity]);

    expect(screen.getByTestId("capacity-reached-banner")).toHaveTextContent(
      "Ask an organization admin to allocate more capacity to this region."
    );
    expect(screen.queryAllByRole("link")).toHaveLength(0);
  });

  it("links to on-demand capacity when region allocation is disabled", () => {
    mockStatusCounts = { queued: 5 };

    renderComponent([FeatureItem.OnDemandCapacity]);

    expect(screen.getByTestId("capacity-reached-banner")).toHaveTextContent("Set critical connections to");
    expect(screen.queryByText(/allocate more capacity/)).not.toBeInTheDocument();
    expect(screen.getByRole("link", { name: "use on-demand capacity" })).toHaveAttribute(
      "href",
      links.dataWorkerOnDemandCapacity
    );
  });

  it("only suggests on-demand capacity when data-worker capacity is unavailable", () => {
    mockStatusCounts = { queued: 5 };
    jest.mocked(useExperiment).mockReturnValue(true);

    renderComponent([FeatureItem.OnDemandCapacity]);

    expect(screen.getByTestId("capacity-reached-banner")).toHaveTextContent("Set critical connections to");
    expect(screen.queryByText(/allocate more capacity/)).not.toBeInTheDocument();
    expect(screen.getByRole("link", { name: "use on-demand capacity" })).toHaveAttribute(
      "href",
      links.dataWorkerOnDemandCapacity
    );
  });

  it("shows only the base message when region allocation and on-demand capacity are disabled", () => {
    mockStatusCounts = { queued: 5 };

    renderComponent();

    expect(screen.getByTestId("capacity-reached-banner")).toHaveTextContent(
      "Maximum capacity reached. Additional syncs will be queued until capacity is available."
    );
    expect(screen.queryAllByRole("link")).toHaveLength(0);
    expect(screen.queryByText(/allocate more capacity/)).not.toBeInTheDocument();
    expect(screen.queryByText(/on-demand capacity/)).not.toBeInTheDocument();
  });
});
