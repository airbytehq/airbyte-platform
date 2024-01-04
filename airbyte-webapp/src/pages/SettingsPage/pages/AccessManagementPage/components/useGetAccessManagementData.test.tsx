import { renderHook } from "@testing-library/react";

import { mockOrganizationUserReadList, mockWorkspaceUserReadList } from "test-utils/mock-data/mockUsersList";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";

import { useCurrentWorkspace, useListUsersInOrganization, useListUsersInWorkspace } from "core/api";

import { useNextGetWorkspaceAccessUsers } from "./useGetAccessManagementData";

jest.mock("core/api");

const mockedUseCurrentWorkspace = useCurrentWorkspace as jest.Mock;
const mockedUseListUsersInOrganization = useListUsersInOrganization as jest.Mock;
const mockedUseListUsersInWorkspace = useListUsersInWorkspace as jest.Mock;

mockedUseCurrentWorkspace.mockReturnValue(mockWorkspace);

describe("useNextGetWorkspaceAccessUsers", () => {
  it("should correctly process and return workspace and organization users", () => {
    mockedUseListUsersInOrganization.mockImplementationOnce(() => {
      return mockOrganizationUserReadList;
    });
    mockedUseListUsersInWorkspace.mockImplementationOnce(() => {
      return mockWorkspaceUserReadList;
    });

    const { result } = renderHook(() => useNextGetWorkspaceAccessUsers());

    expect(result.current.workspace).toBeDefined();
    expect(result.current.workspace?.users).toHaveLength(3);
    // user with both workspace_admin and organization_member permissions
    expect(result.current.workspace?.users[0].workspacePermission?.permissionType).toEqual("workspace_admin");
    expect(result.current.workspace?.users[0].organizationPermission?.permissionType).toEqual("organization_member");
    // user with only workspace_reader and no organization permissions
    expect(result.current.workspace?.users[1].workspacePermission?.permissionType).toEqual("workspace_reader");
    expect(result.current.workspace?.users[1].organizationPermission).toBeUndefined();
    // user with only organization_admin and no workspace permissions
    expect(result.current.workspace?.users[2].workspacePermission).toBeUndefined();
    expect(result.current.workspace?.users[2].organizationPermission?.permissionType).toEqual("organization_admin");
  });
  it("should correctly process and return workspace and organization users with empty organization users list", () => {
    mockedUseListUsersInOrganization.mockImplementationOnce(() => {
      return { users: [] };
    });
    mockedUseListUsersInWorkspace.mockImplementationOnce(() => {
      return mockWorkspaceUserReadList;
    });

    const { result } = renderHook(() => useNextGetWorkspaceAccessUsers());

    expect(result.current.workspace).toBeDefined();
    expect(result.current.workspace?.users).toHaveLength(2);
    expect(result.current.workspace?.users[0].workspacePermission?.permissionType).toEqual("workspace_admin");
    expect(result.current.workspace?.users[0].organizationPermission).toBeUndefined();
    expect(result.current.workspace?.users[1].workspacePermission?.permissionType).toEqual("workspace_reader");
    expect(result.current.workspace?.users[1].organizationPermission).toBeUndefined();
  });
  it("should correctly process and return workspace and organization users with empty workspace users list", () => {
    mockedUseListUsersInOrganization.mockImplementationOnce(() => {
      return mockOrganizationUserReadList;
    });
    mockedUseListUsersInWorkspace.mockImplementationOnce(() => {
      return { users: [] };
    });

    const { result } = renderHook(() => useNextGetWorkspaceAccessUsers());

    expect(result.current.workspace).toBeDefined();
    expect(result.current.workspace?.users).toHaveLength(1);

    expect(result.current.workspace?.users[0].workspacePermission).toBeUndefined();
    expect(result.current.workspace?.users[0].organizationPermission?.permissionType).toEqual("organization_admin");
  });
});
