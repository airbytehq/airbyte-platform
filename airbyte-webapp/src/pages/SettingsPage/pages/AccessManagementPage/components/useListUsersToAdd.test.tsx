import { renderHook } from "@testing-library/react";

import {
  mockOrganizationUsers,
  mockWorkspaceAccessUsers,
  mockWorkspaceUserInvitations,
} from "test-utils/mock-data/mockUsersList";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useListUserInvitations, useListUsersInOrganization, useListWorkspaceAccessUsers } from "core/api";
import { useGeneratedIntent, useIntent } from "core/utils/rbac";

import { useListUsersToAdd } from "./useListUsersToAdd";

jest.mock("core/api", () => ({
  useListUsersInOrganization: jest.fn(),
  useListWorkspaceAccessUsers: jest.fn(),
  useListUserInvitations: jest.fn(),
}));

jest.mock("core/utils/rbac", () => ({
  useIntent: jest.fn(),
  useGeneratedIntent: jest.fn(),
  Intent: {
    CreateOrEditConnection: "CreateOrEditConnection",
  },
}));

jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceId: jest.fn(),
}));

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
}));

describe("#useListUsersToAdd", () => {
  beforeEach(() => {
    (useCurrentWorkspaceId as jest.Mock).mockReturnValue("ws-id");
    (useCurrentOrganizationId as jest.Mock).mockReturnValue("org-id");
    (useListUsersInOrganization as jest.Mock).mockReturnValue({
      users: mockOrganizationUsers,
    });
    (useListWorkspaceAccessUsers as jest.Mock).mockReturnValue({
      usersWithAccess: mockWorkspaceAccessUsers,
    });
    (useIntent as jest.Mock).mockReturnValue(true);
    (useGeneratedIntent as jest.Mock).mockReturnValue(true);
    (useListUserInvitations as jest.Mock).mockReturnValue(mockWorkspaceUserInvitations);
  });

  describe("Scope: workspace", () => {
    it("when no search term, should return users who are not org admin and have no workspace permission", () => {
      const { result } = renderHook(() => useListUsersToAdd("workspace", ""));

      const mappedUsers = result.current;

      expect(mappedUsers).toEqual([
        {
          organizationPermission: {
            permissionType: "organization_reader",
            permissionId: "permission2",
            organizationId: "org-id",
            userId: "orgUser2",
          },
          id: "orgUser2",
          userName: "Org User 2",
          userEmail: "orguser2@test.com",
          workspacePermission: undefined,
        },
        {
          id: "orgUser1",
          userName: "Org User 1",
          userEmail: "orguser1@test.com",
          organizationPermission: {
            permissionId: "permission1",
            permissionType: "organization_member",
            organizationId: "org-id",
            userId: "orgUser1",
          },
          workspacePermission: undefined,
        },
      ]);
    });
    it("when search term present, should return users who are not org admin and have no workspace permission that match (full list)", () => {
      const { result } = renderHook(() => useListUsersToAdd("workspace", "u"));

      const mappedUsers = result.current;

      expect(mappedUsers).toEqual([
        {
          id: "workspaceUser1",
          userName: "Ws User 1",
          userEmail: "wsuser1@test.com",
          organizationPermission: undefined,
          workspacePermission: {
            permissionType: "workspace_admin",
            permissionId: "permission4",
            userId: "workspaceUser1",
          },
        },
        {
          id: "orgUser2",
          userName: "Org User 2",
          userEmail: "orguser2@test.com",
          workspacePermission: undefined,
          organizationPermission: {
            permissionType: "organization_reader",
            permissionId: "permission2",
            organizationId: "org-id",
            userId: "orgUser2",
          },
        },
        {
          id: "orgAndWorkspaceUser",
          userName: "Org And Workspace User",
          userEmail: "organdworkspaceuser@test.com",
          workspacePermission: {
            permissionType: "workspace_editor",
            permissionId: "permission5",
            userId: "orgAndWorkspaceUser",
          },
          organizationPermission: {
            userId: "orgAndWorkspaceUser",
            permissionId: "permission3",
            permissionType: "organization_member",
          },
        },
        {
          id: "orgUser1",
          userName: "Org User 1",
          userEmail: "orguser1@test.com",
          organizationPermission: {
            permissionType: "organization_member",
            permissionId: "permission1",
            organizationId: "org-id",
            userId: "orgUser1",
          },
        },
        {
          id: "code1",
          invitationPermissionType: "workspace_reader",
          invitationStatus: "pending",
          userEmail: "userInvite1@example.com",
        },
      ]);
    });
    it("when search term present, should return users who are not org admin and have no workspace permission that match (partial list)", () => {
      const { result } = renderHook(() => useListUsersToAdd("workspace", "Org User"));

      const mappedUsers = result.current;

      expect(mappedUsers).toEqual([
        {
          id: "orgUser2",
          userName: "Org User 2",
          userEmail: "orguser2@test.com",
          workspacePermission: undefined,
          organizationPermission: {
            permissionType: "organization_reader",
            permissionId: "permission2",
            organizationId: "org-id",
            userId: "orgUser2",
          },
        },
        {
          id: "orgUser1",
          userName: "Org User 1",
          userEmail: "orguser1@test.com",
          organizationPermission: {
            permissionType: "organization_member",
            permissionId: "permission1",
            organizationId: "org-id",
            userId: "orgUser1",
          },
        },
      ]);
    });
  });

  describe("Scope: organization", () => {
    it("when no search term, should list no users", () => {
      const { result } = renderHook(() => useListUsersToAdd("organization", ""));

      const mappedUsers = result.current;

      expect(mappedUsers).toEqual([]);
    });
    it("when search term present, should return users who are not org admin and have no workspace permission that match (full list)", () => {
      const { result } = renderHook(() => useListUsersToAdd("organization", "User"));

      const mappedUsers = result.current;

      expect(mappedUsers).toEqual([
        {
          id: "orgUser1",
          userName: "Org User 1",
          userEmail: "orguser1@test.com",
          organizationPermission: {
            permissionType: "organization_member",
            permissionId: "permission1",
            organizationId: "org-id",
            userId: "orgUser1",
          },
        },
        {
          id: "orgUser2",
          userName: "Org User 2",
          userEmail: "orguser2@test.com",
          organizationPermission: {
            permissionType: "organization_reader",
            permissionId: "permission2",
            organizationId: "org-id",
            userId: "orgUser2",
          },
        },
        {
          id: "orgUser3",
          userName: "Org User 3",
          userEmail: "orguser3@test.com",
          organizationPermission: {
            permissionType: "organization_admin",
            permissionId: "permission6",
            organizationId: "org-id",
            userId: "orgUser3",
          },
        },
        {
          id: "orgAndWorkspaceUser",
          userName: "Org And Workspace User",
          userEmail: "organdworkspaceuser@test.com",
          organizationPermission: {
            permissionType: "organization_member",
            permissionId: "permission3",
            organizationId: "org-id",
            userId: "orgAndWorkspaceUser",
          },
        },
        {
          id: "code1",
          invitationPermissionType: "workspace_reader",
          invitationStatus: "pending",
          userEmail: "userInvite1@example.com",
        },
      ]);
    });
    it("when search term present, should return users who are not org admin and have no workspace permission that match (partial list)", () => {
      const { result } = renderHook(() => useListUsersToAdd("organization", "Org User"));

      const mappedUsers = result.current;

      expect(mappedUsers).toEqual([
        {
          id: "orgUser1",
          userName: "Org User 1",
          userEmail: "orguser1@test.com",
          organizationPermission: {
            permissionType: "organization_member",
            permissionId: "permission1",
            organizationId: "org-id",
            userId: "orgUser1",
          },
        },
        {
          id: "orgUser2",
          userName: "Org User 2",
          userEmail: "orguser2@test.com",
          organizationPermission: {
            permissionType: "organization_reader",
            permissionId: "permission2",
            organizationId: "org-id",
            userId: "orgUser2",
          },
        },
        {
          id: "orgUser3",
          userName: "Org User 3",
          userEmail: "orguser3@test.com",
          organizationPermission: {
            permissionType: "organization_admin",
            permissionId: "permission6",
            organizationId: "org-id",
            userId: "orgUser3",
          },
        },
      ]);
    });
  });
});
