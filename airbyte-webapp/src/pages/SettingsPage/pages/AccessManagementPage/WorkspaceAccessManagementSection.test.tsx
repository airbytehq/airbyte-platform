import { screen } from "@testing-library/react";

import { render } from "test-utils";

import {
  useCurrentOrganizationInfo,
  useCurrentWorkspace,
  useListUserInvitations,
  useListWorkspaceAccessUsers,
} from "core/api";
import { useGeneratedIntent } from "core/utils/rbac";

import WorkspaceAccessManagementSection from "./WorkspaceAccessManagementSection";

// core/api's import graph is circular, so a jest.requireActual spread fails at module evaluation
// (same rationale as ScimSettingsCard.test.tsx) - list the exports this component uses explicitly.
jest.mock("core/api", () => ({
  useCurrentOrganizationInfo: jest.fn(),
  useCurrentWorkspace: jest.fn(),
  useListUserInvitations: jest.fn(),
  useListWorkspaceAccessUsers: jest.fn(),
}));

// Intent is re-declared rather than spread from the real module: core/utils/rbac reaches core/api,
// so requireActual reintroduces the circular import this file is avoiding.
jest.mock("core/utils/rbac", () => ({
  Intent: { UpdateWorkspacePermissions: "UpdateWorkspacePermissions" },
  useGeneratedIntent: jest.fn(),
}));

// Stubbed so the section's own SCIM branches are what these tests exercise, rather than the table's
// per-row rendering (covered in RoleManagementCell.test.tsx).
jest.mock("./WorkspaceUsersTable", () => ({
  WorkspaceUsersTable: () => <div data-testid="workspace-users-table" />,
}));

const mockUseCurrentOrganizationInfo = useCurrentOrganizationInfo as jest.Mock;
const mockUseCurrentWorkspace = useCurrentWorkspace as jest.Mock;
const mockUseListUserInvitations = useListUserInvitations as jest.Mock;
const mockUseListWorkspaceAccessUsers = useListWorkspaceAccessUsers as jest.Mock;
const mockUseGeneratedIntent = useGeneratedIntent as jest.Mock;

const SCIM_BANNER =
  "Workspace membership is controlled by your identity provider. Edit membership in your identity provider.";

const setScim = (scim: boolean) => {
  mockUseCurrentOrganizationInfo.mockReturnValue({
    organizationId: "org-id",
    organizationName: "Org",
    sso: false,
    scim,
  });
};

describe(`${WorkspaceAccessManagementSection.name}`, () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseCurrentWorkspace.mockReturnValue({ workspaceId: "workspace-id", name: "Workspace" });
    mockUseListWorkspaceAccessUsers.mockReturnValue({ usersWithAccess: [] });
    mockUseListUserInvitations.mockReturnValue([]);
    mockUseGeneratedIntent.mockReturnValue(true);
  });

  describe("when SCIM is enabled", () => {
    beforeEach(() => setScim(true));

    it("renders the provider-managed banner", async () => {
      await render(<WorkspaceAccessManagementSection />);

      expect(screen.getByText(SCIM_BANNER)).toBeInTheDocument();
    });

    it("renders the SCIM badge", async () => {
      await render(<WorkspaceAccessManagementSection />);

      expect(screen.getByText("SCIM enabled")).toBeInTheDocument();
    });

    it("hides the invite action", async () => {
      await render(<WorkspaceAccessManagementSection />);

      expect(screen.queryByRole("button", { name: /new member/i })).not.toBeInTheDocument();
    });
  });

  describe("when SCIM is disabled", () => {
    beforeEach(() => setScim(false));

    it("does not render the provider-managed banner", async () => {
      await render(<WorkspaceAccessManagementSection />);

      expect(screen.queryByText(SCIM_BANNER)).not.toBeInTheDocument();
    });

    it("does not render the SCIM badge", async () => {
      await render(<WorkspaceAccessManagementSection />);

      expect(screen.queryByText("SCIM enabled")).not.toBeInTheDocument();
    });

    it("shows the invite action", async () => {
      await render(<WorkspaceAccessManagementSection />);

      expect(screen.getByRole("button", { name: /new member/i })).toBeInTheDocument();
    });
  });

  it("uses the non-SCIM controls while organization info is unavailable", async () => {
    mockUseCurrentOrganizationInfo.mockReturnValue(undefined);

    await render(<WorkspaceAccessManagementSection />);

    expect(screen.getByRole("button", { name: /new member/i })).toBeInTheDocument();
    expect(screen.queryByText(SCIM_BANNER)).not.toBeInTheDocument();
    expect(screen.queryByText("SCIM enabled")).not.toBeInTheDocument();
  });
});
