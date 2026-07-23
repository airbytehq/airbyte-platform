import { act } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { mocked, render } from "test-utils";

import {
  useCancelSubscription,
  useCurrentWorkspace,
  useDeleteWorkspace,
  useGetOrganizationSubscriptionInfo,
  useIsLastWorkspaceInOrganization,
} from "core/api";
import { useOrganizationSubscriptionStatus } from "core/utils/useOrganizationSubscriptionStatus";

import { DeleteWorkspace } from "./DeleteWorkspace";

const mockNavigate = jest.fn();
const mockCloseConfirmationModal = jest.fn();
const mockOpenConfirmationModal = jest.fn();
let mockConfirmationSubmit: (() => Promise<void>) | undefined;

jest.mock("react-router-dom", () => ({
  ...jest.requireActual("react-router-dom"),
  useNavigate: () => mockNavigate,
}));

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn().mockReturnValue("test-organization-id"),
}));

jest.mock("core/api", () => ({
  useCancelSubscription: jest.fn(),
  useCurrentWorkspace: jest.fn(),
  useDeleteWorkspace: jest.fn(),
  useGetOrganizationSubscriptionInfo: jest.fn(),
  useIsLastWorkspaceInOrganization: jest.fn(),
}));

jest.mock("core/services/ConfirmationModal", () => ({
  ...jest.requireActual("core/services/ConfirmationModal"),
  useConfirmationModalService: () => ({
    openConfirmationModal: mockOpenConfirmationModal,
    closeConfirmationModal: mockCloseConfirmationModal,
  }),
}));

jest.mock("core/utils/useOrganizationSubscriptionStatus", () => ({
  useOrganizationSubscriptionStatus: jest.fn(),
}));

describe("DeleteWorkspace", () => {
  const mockDeleteWorkspace = jest.fn();
  const mockCancelSubscription = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
    mockConfirmationSubmit = undefined;
    mockOpenConfirmationModal.mockImplementation((options) => {
      mockConfirmationSubmit = options.onSubmit;
    });
    mockDeleteWorkspace.mockResolvedValue(undefined);
    mockCancelSubscription.mockResolvedValue(undefined);
    mocked(useCurrentWorkspace).mockReturnValue({
      workspaceId: "test-workspace-id",
      organizationId: "test-organization-id",
      name: "Test Workspace",
    } as ReturnType<typeof useCurrentWorkspace>);
    mocked(useDeleteWorkspace).mockReturnValue({
      mutateAsync: mockDeleteWorkspace,
      isLoading: false,
    } as unknown as ReturnType<typeof useDeleteWorkspace>);
    mocked(useCancelSubscription).mockReturnValue({
      mutateAsync: mockCancelSubscription,
      isLoading: false,
    } as unknown as ReturnType<typeof useCancelSubscription>);
    mocked(useIsLastWorkspaceInOrganization).mockReturnValue({
      isLastWorkspace: true,
      isLoading: false,
      error: null,
    });
    mocked(useOrganizationSubscriptionStatus).mockReturnValue({
      subscriptionStatus: "subscribed",
    } as ReturnType<typeof useOrganizationSubscriptionStatus>);
  });

  it("cancels the subscription when deleting the last workspace for a self-serve subscription", async () => {
    mocked(useGetOrganizationSubscriptionInfo).mockReturnValue({
      data: {
        name: "Airbyte Plus",
        selfServeSubscription: true,
        upcomingInvoice: {
          dueDate: "2026-06-01T00:00:00Z",
        },
      },
      isLoading: false,
    } as unknown as ReturnType<typeof useGetOrganizationSubscriptionInfo>);

    const wrapper = await render(<DeleteWorkspace />);
    await userEvent.click(wrapper.getByRole("button", { name: "Delete your workspace" }));

    expect(mockOpenConfirmationModal).toHaveBeenCalledTimes(1);
    await act(async () => {
      await mockConfirmationSubmit?.();
    });

    expect(mockDeleteWorkspace).toHaveBeenCalledWith("test-workspace-id");
    expect(mockCancelSubscription).toHaveBeenCalledTimes(1);
    expect(mockNavigate).toHaveBeenCalledWith("/organization/test-organization-id");
  });

  it("does not cancel the subscription when deleting the last workspace for a non-self-serve subscription", async () => {
    mocked(useGetOrganizationSubscriptionInfo).mockReturnValue({
      data: {
        name: "Enterprise",
        selfServeSubscription: false,
      },
      isLoading: false,
    } as unknown as ReturnType<typeof useGetOrganizationSubscriptionInfo>);

    const wrapper = await render(<DeleteWorkspace />);
    await userEvent.click(wrapper.getByRole("button", { name: "Delete your workspace" }));

    await act(async () => {
      await mockConfirmationSubmit?.();
    });

    expect(mockDeleteWorkspace).toHaveBeenCalledWith("test-workspace-id");
    expect(mockCancelSubscription).not.toHaveBeenCalled();
  });
});
