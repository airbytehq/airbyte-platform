import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { TestWrapper } from "test-utils/testutils";

import { FeatureItem } from "core/services/features";
import * as FeatureService from "core/services/features/FeatureService";
import * as ModalService from "core/services/Modal/ModalService";
import { useGeneratedIntent } from "core/utils/rbac";

import { InviteUsersButton } from "./InviteUsersButton";

jest.mock("core/api", () => ({
  useCurrentWorkspace: () => ({ workspaceId: "workspaceId", name: "My workspace" }),
}));

jest.mock("core/utils/rbac", () => ({
  useGeneratedIntent: jest.fn(),
  Intent: {
    UpdateWorkspacePermissions: "UpdateWorkspacePermissions",
  },
}));

const mockShowInviteUsers = (enabled: boolean) =>
  jest.spyOn(FeatureService, "useFeature").mockImplementation((key: FeatureItem) => {
    if (key === FeatureItem.ShowInviteUsersHint) {
      return enabled;
    }
    throw new Error(`${key} is not mocked`);
  });

describe("InviteUsersButton", () => {
  beforeEach(() => {
    jest.restoreAllMocks();
    jest.mocked(useGeneratedIntent).mockReturnValue(true);
  });

  it("shows when the feature is on and the user can manage workspace members", async () => {
    mockShowInviteUsers(true);

    render(<InviteUsersButton />, { wrapper: TestWrapper });

    const button = screen.getByRole("button", { name: "Invite users" });
    await userEvent.hover(button);
    expect(await screen.findByRole("tooltip")).toHaveTextContent("Invite users");
    expect(useGeneratedIntent).toHaveBeenCalledWith("UpdateWorkspacePermissions");
  });

  it("does not show when the feature is off", () => {
    mockShowInviteUsers(false);

    render(<InviteUsersButton />, { wrapper: TestWrapper });

    expect(screen.queryByRole("button", { name: "Invite users" })).not.toBeInTheDocument();
  });

  it("does not show when the user cannot manage workspace members", () => {
    mockShowInviteUsers(true);
    jest.mocked(useGeneratedIntent).mockReturnValue(false);

    render(<InviteUsersButton />, { wrapper: TestWrapper });

    expect(screen.queryByRole("button", { name: "Invite users" })).not.toBeInTheDocument();
  });

  it("disables the button when asked and keeps its tooltip", async () => {
    mockShowInviteUsers(true);

    render(<InviteUsersButton disabled />, { wrapper: TestWrapper });

    const button = screen.getByRole("button", { name: "Invite users" });
    expect(button).toBeDisabled();

    await userEvent.hover(button.parentElement as HTMLElement);
    expect(await screen.findByRole("tooltip")).toHaveTextContent("Invite users");
  });

  it("opens the add-member modal for the current workspace on click", async () => {
    const mockOpenModal = jest.fn();
    jest.spyOn(ModalService, "useModalService").mockImplementation(() => ({
      openModal: mockOpenModal,
      getCurrentModalTitle: jest.fn(),
    }));
    mockShowInviteUsers(true);

    render(<InviteUsersButton />, { wrapper: TestWrapper });
    await userEvent.click(screen.getByRole("button", { name: "Invite users" }));

    expect(mockOpenModal).toHaveBeenCalledTimes(1);
    expect(mockOpenModal).toHaveBeenCalledWith(expect.objectContaining({ title: "Add a member to My workspace" }));
  });
});
