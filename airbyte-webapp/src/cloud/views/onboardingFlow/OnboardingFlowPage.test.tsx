import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useSyncExternalStore } from "react";
import { flushSync } from "react-dom";
import { Route, Routes } from "react-router-dom";

import { TestWrapper } from "test-utils/testutils";

import { OrganizationOnboardingStatus } from "core/api/types/AirbyteClient";

import OnboardingFlowPage from "./OnboardingFlowPage";

const mockOnboardingStatus = (() => {
  let status: OrganizationOnboardingStatus = "choose_method";
  const listeners = new Set<() => void>();
  const subscribe = (listener: () => void) => {
    listeners.add(listener);
    return () => listeners.delete(listener);
  };
  const getStatus = () => status;
  return {
    get: getStatus,
    set: (nextStatus: OrganizationOnboardingStatus) => {
      status = nextStatus;
      listeners.forEach((listener) => listener());
    },
    useStatus: () => useSyncExternalStore(subscribe, getStatus),
  };
})();

interface MockUpdateOptions {
  onSuccess?: () => void;
}

/** As in TanStack Query, the callbacks run first, and then the new status re-renders the gate. */
const mockUpdateOnboarding = jest.fn(
  ({ status }: { status: OrganizationOnboardingStatus }, options?: MockUpdateOptions) => {
    options?.onSuccess?.();
    flushSync(() => mockOnboardingStatus.set(status));
  }
);
const mockFetchOrganizationInfo = jest.fn(async () => ({ onboardingStatus: mockOnboardingStatus.get() }));
const mockConnectionStatus = jest.fn<{ status: string; activeJob?: { id: number } }, []>();
const mockSyncConnection = jest.fn((_connection: unknown, options?: MockUpdateOptions) => options?.onSuccess?.());

jest.mock("area/organization/utils", () => ({ useCurrentOrganizationId: () => "org-1" }));
jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceId: () => "workspace-1",
  useCurrentWorkspaceLink: () => (path: string) => `/workspaces/workspace-1${path}`,
}));
jest.mock("area/connection/components/ConnectionStatus/useConnectionStatus", () => ({
  useConnectionStatus: () => mockConnectionStatus(),
}));
jest.mock("cloud/components/AdpOrganizationAccessGuard", () => ({
  AdpOrganizationAccessGuard: ({ children }: React.PropsWithChildren) => <>{children}</>,
}));
jest.mock("core/api/cloud", () => ({ usePrefetchWorkspaceData: () => undefined }));
jest.mock("core/api", () => ({
  useCurrentWorkspace: () => ({ workspaceId: "workspace-1", organizationId: "org-1" }),
  useCurrentOrganizationInfo: () => ({ onboardingStatus: mockOnboardingStatus.useStatus() }),
  useUpdateOrganizationOnboarding: () => ({ mutate: mockUpdateOnboarding, isLoading: false }),
  useFetchOrganizationInfo: () => mockFetchOrganizationInfo,
  useDestinationDefinitionList: () => ({ destinationDefinitionMap: new Map() }),
  useFirstSourcePage: () => [{ sourceId: "source-1", sourceName: "File", isDraft: false }],
  useFirstDestinationPage: () => [
    { destinationId: "destination-1", destinationName: "End-to-End Testing", isDraft: false },
  ],
  useFirstConnectionBetween: () => ({ connectionId: "connection-1" }),
  useSyncConnection: () => ({ mutate: mockSyncConnection, isLoading: false }),
}));
jest.mock("cloud/area/workspace/utils/useRegisterWorkspaceContext", () => ({
  useRegisterWorkspaceContext: () => undefined,
}));
jest.mock("./OnboardingHeader", () => ({ OnboardingHeader: () => null }));
jest.mock("area/connector/components/ConnectorIcon", () => ({ ConnectorIcon: () => null }));
jest.mock("./useShouldEnterOnboardingFlow", () => ({
  ...jest.requireActual("./useShouldEnterOnboardingFlow"),
  useCanOpenOnboardingFlow: () => !["completed", "skipped"].includes(mockOnboardingStatus.useStatus()),
}));
jest.mock("./useSuggestedSource", () => ({ useSuggestedSource: () => undefined }));

const renderFlow = (status: OrganizationOnboardingStatus) => {
  mockOnboardingStatus.set(status);
  render(
    <TestWrapper route="/workspaces/workspace-1/onboarding-flow">
      <Routes>
        <Route path="workspaces/:workspaceId/onboarding-flow" element={<OnboardingFlowPage />} />
        <Route path="workspaces/:workspaceId" element={<div data-testid="workspace-page" />} />
      </Routes>
    </TestWrapper>
  );
};

describe("OnboardingFlowPage", () => {
  beforeEach(() => {
    mockConnectionStatus.mockReturnValue({ status: "pending" });
  });

  it("stays on the MCP install screen after the AI agent path completes onboarding, and Finished opens the normal app", async () => {
    renderFlow("choose_method");

    await userEvent.click(screen.getByText("My AI agent"));
    await userEvent.click(screen.getByRole("button", { name: "Continue" }));

    expect(await screen.findByRole("heading", { name: "Install MCP" })).toBeInTheDocument();
    expect(await screen.findByText(/Help me set up my first Airbyte connection/)).toBeInTheDocument();
    expect(screen.queryByTestId("workspace-page")).not.toBeInTheDocument();

    await userEvent.click(screen.getByRole("link", { name: "Finished" }));

    expect(await screen.findByTestId("workspace-page")).toBeInTheDocument();
  });

  it("opens the normal app when the member skips onboarding", async () => {
    renderFlow("choose_method");

    await userEvent.click(screen.getByRole("button", { name: "Skip onboarding" }));

    expect(await screen.findByTestId("workspace-page")).toBeInTheDocument();
    expect(mockUpdateOnboarding).toHaveBeenCalledWith(
      { organizationId: "org-1", status: "skipped" },
      expect.anything()
    );
  });

  it("stays on the first sync after Run your first sync completes onboarding", async () => {
    renderFlow("run_first_sync");

    await userEvent.click(await screen.findByRole("button", { name: "Run your first sync" }));

    expect(await screen.findByRole("link", { name: "Monitor sync" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "First sync running" })).toBeInTheDocument();
    expect(screen.queryByTestId("workspace-page")).not.toBeInTheDocument();
  });

  it("only saves completed when Run your first sync is clicked while the first job is still active", async () => {
    mockSyncConnection.mockClear();
    mockUpdateOnboarding.mockClear();
    mockConnectionStatus.mockReturnValue({ status: "pending", activeJob: { id: 1 } });
    renderFlow("run_first_sync");

    await userEvent.click(await screen.findByRole("button", { name: "Run your first sync" }));

    expect(mockSyncConnection).not.toHaveBeenCalled();
    expect(mockUpdateOnboarding).toHaveBeenCalledWith(
      { organizationId: "org-1", status: "completed" },
      expect.anything()
    );
  });

  it("opens the normal app and saves nothing when onboarding was skipped in another tab or device meanwhile", async () => {
    renderFlow("run_first_sync");
    mockUpdateOnboarding.mockClear();
    mockFetchOrganizationInfo.mockResolvedValueOnce({ onboardingStatus: "skipped" });

    await userEvent.click(await screen.findByRole("button", { name: "Back" }));

    expect(await screen.findByTestId("workspace-page")).toBeInTheDocument();
    expect(mockUpdateOnboarding).not.toHaveBeenCalled();
  });
});
