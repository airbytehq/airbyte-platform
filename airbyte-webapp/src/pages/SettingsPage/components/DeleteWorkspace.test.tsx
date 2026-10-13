import { act } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { mocked, render } from "test-utils";

import { useCurrentWorkspace, useDeleteWorkspace, useIsLastWorkspaceInOrganization } from "core/api";

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
  useCurrentWorkspace: jest.fn(),
  useDeleteWorkspace: jest.fn(),
  useIsLastWorkspaceInOrganization: jest.fn(),
}));

jest.mock("core/services/ConfirmationModal", () => ({
  ...jest.requireActual("core/services/ConfirmationModal"),
  useConfirmationModalService: () => ({
    openConfirmationModal: mockOpenConfirmationModal,
    closeConfirmationModal: mockCloseConfirmationModal,
  }),
}));

describe("DeleteWorkspace", () => {
  const mockDeleteWorkspace = jest.fn();

  const mockIsLastWorkspace = (isLastWorkspace: boolean) =>
    mocked(useIsLastWorkspaceInOrganization).mockReturnValue({ isLastWorkspace, isLoading: false, error: null });

  beforeEach(() => {
    jest.clearAllMocks();
    mockConfirmationSubmit = undefined;
    mockOpenConfirmationModal.mockImplementation((options) => {
      mockConfirmationSubmit = options.onSubmit;
    });
    mockDeleteWorkspace.mockResolvedValue(undefined);
    mocked(useCurrentWorkspace).mockReturnValue({
      workspaceId: "test-workspace-id",
      organizationId: "test-organization-id",
      name: "Test Workspace",
    } as ReturnType<typeof useCurrentWorkspace>);
    mocked(useDeleteWorkspace).mockReturnValue({
      mutateAsync: mockDeleteWorkspace,
      isLoading: false,
    } as unknown as ReturnType<typeof useDeleteWorkspace>);
  });

  it("deletes the workspace and returns to the organization when other workspaces exist", async () => {
    mockIsLastWorkspace(false);

    const wrapper = await render(<DeleteWorkspace />);
    await userEvent.click(wrapper.getByRole("button", { name: "Delete your workspace" }));

    expect(mockOpenConfirmationModal).toHaveBeenCalledTimes(1);
    await act(async () => {
      await mockConfirmationSubmit?.();
    });

    expect(mockDeleteWorkspace).toHaveBeenCalledWith("test-workspace-id");
    expect(mockNavigate).toHaveBeenCalledWith("/organization/test-organization-id");
  });

  it("disables deletion of the organization's last workspace", async () => {
    mockIsLastWorkspace(true);

    const wrapper = await render(<DeleteWorkspace />);

    expect(wrapper.getByRole("button", { name: "Delete your workspace" })).toBeDisabled();
    expect(wrapper.getByText(/only workspace/)).toBeInTheDocument();

    await userEvent.click(wrapper.getByRole("button", { name: "Delete your workspace" }));
    expect(mockOpenConfirmationModal).not.toHaveBeenCalled();
    expect(mockDeleteWorkspace).not.toHaveBeenCalled();
  });
});
