import { renderHook } from "@testing-library/react";

import { mockUser } from "test-utils/mock-data/mockUser";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";

import { useListPermissions, useCurrentWorkspace } from "core/api";
import { PermissionRead, WorkspaceRead } from "core/request/AirbyteClient";

import { useRbac } from "./rbac";
import { RbacPermission, useRbacPermissionsQuery } from "./rbacPermissionsQuery";

jest.mock("core/services/auth", () => ({
  useCurrentUser: () => mockUser,
}));

jest.mock("./rbacPermissionsQuery", () => ({
  useRbacPermissionsQuery: jest.fn(),
}));
const mockUseRbacPermissionsQuery = useRbacPermissionsQuery as unknown as jest.Mock;

jest.mock("core/api", () => {
  const actual = jest.requireActual("core/api");
  return {
    ...actual,
    useListPermissions: jest.fn(() => ({ permissions: [] })),
    useCurrentWorkspace: jest.fn(() => ({ ...mockWorkspace, organizationId: "test-organization" })),
  };
});

const mockUseListPermissions = useListPermissions as unknown as jest.Mock<{ permissions: RbacPermission[] }>;

const mockUseCurrentWorkspace = useCurrentWorkspace as unknown as jest.Mock<WorkspaceRead>;
mockUseCurrentWorkspace.mockImplementation(() => ({
  ...mockWorkspace,
  workspaceId: "test-workspace",
  organizationId: "test-organization",
}));

describe("useRbac", () => {
  it("passes permissions", () => {
    mockUseRbacPermissionsQuery.mockClear();

    const permissions: PermissionRead[] = [
      { permissionId: "", userId: "", permissionType: "instance_admin" },
      { permissionId: "", userId: "", permissionType: "workspace_reader", organizationId: "work-18" },
      { permissionId: "", userId: "", permissionType: "organization_editor", organizationId: "org-1" },
    ];

    mockUseListPermissions.mockImplementation(() => ({
      permissions: [...permissions],
    }));

    renderHook(() => useRbac({ resourceType: "INSTANCE", role: "ADMIN" }));
    expect(mockUseRbacPermissionsQuery).toHaveBeenCalledTimes(1);
    expect(mockUseRbacPermissionsQuery.mock.lastCall?.[0]).toEqual(permissions);
  });

  describe("query assembly", () => {
    it("no permissions", () => {
      mockUseListPermissions.mockImplementation(() => ({
        permissions: [],
      }));

      mockUseRbacPermissionsQuery.mockClear();
      renderHook(() => useRbac({ resourceType: "INSTANCE", role: "ADMIN" }));
      expect(mockUseRbacPermissionsQuery).toHaveBeenCalledTimes(1);
      expect(mockUseRbacPermissionsQuery.mock.lastCall?.[1]).toEqual({ resourceType: "INSTANCE", role: "ADMIN" });
    });

    it("instance admin does not need to add details to the query", () => {
      mockUseListPermissions.mockImplementation(() => ({
        permissions: [{ permissionType: "instance_admin" }],
      }));

      mockUseRbacPermissionsQuery.mockClear();
      renderHook(() => useRbac({ resourceType: "INSTANCE", role: "ADMIN" }));
      expect(mockUseRbacPermissionsQuery).toHaveBeenCalledTimes(1);
      expect(mockUseRbacPermissionsQuery.mock.lastCall?.[1]).toEqual({ resourceType: "INSTANCE", role: "ADMIN" });
    });

    it("organizationId is found by the context", () => {
      mockUseListPermissions.mockImplementation(() => ({
        permissions: [{ permissionType: "instance_admin" }],
      }));

      mockUseRbacPermissionsQuery.mockClear();
      renderHook(() => useRbac({ resourceType: "ORGANIZATION", role: "ADMIN" }));
      expect(mockUseRbacPermissionsQuery).toHaveBeenCalledTimes(1);
      expect(mockUseRbacPermissionsQuery.mock.lastCall?.[1]).toEqual({
        resourceType: "ORGANIZATION",
        role: "ADMIN",
        resourceId: "test-organization",
      });
    });

    it("organizationId can be provided directly", () => {
      mockUseListPermissions.mockImplementation(() => ({
        permissions: [{ permissionType: "instance_admin" }],
      }));

      mockUseRbacPermissionsQuery.mockClear();
      renderHook(() => useRbac({ resourceType: "ORGANIZATION", role: "ADMIN", resourceId: "some-other-organization" }));
      expect(mockUseRbacPermissionsQuery).toHaveBeenCalledTimes(1);
      expect(mockUseRbacPermissionsQuery.mock.lastCall?.[1]).toEqual({
        resourceType: "ORGANIZATION",
        role: "ADMIN",
        resourceId: "some-other-organization",
      });
    });

    it("workspaceId is found by the context", () => {
      mockUseListPermissions.mockImplementation(() => ({
        permissions: [{ permissionType: "instance_admin" }],
      }));

      mockUseRbacPermissionsQuery.mockClear();
      renderHook(() => useRbac({ resourceType: "WORKSPACE", role: "ADMIN" }));
      expect(mockUseRbacPermissionsQuery).toHaveBeenCalledTimes(1);
      expect(mockUseRbacPermissionsQuery.mock.lastCall?.[1]).toEqual({
        resourceType: "WORKSPACE",
        role: "ADMIN",
        resourceId: "test-workspace",
      });
    });

    it("workspaceId can be provided directly", () => {
      mockUseListPermissions.mockImplementation(() => ({
        permissions: [{ permissionType: "instance_admin" }],
      }));

      mockUseRbacPermissionsQuery.mockClear();
      renderHook(() => useRbac({ resourceType: "WORKSPACE", role: "ADMIN", resourceId: "some-other-workspace" }));
      expect(mockUseRbacPermissionsQuery).toHaveBeenCalledTimes(1);
      expect(mockUseRbacPermissionsQuery.mock.lastCall?.[1]).toEqual({
        resourceType: "WORKSPACE",
        role: "ADMIN",
        resourceId: "some-other-workspace",
      });
    });
  });

  describe("degenerate cases", () => {
    let existingWorkspaceMock: typeof mockUseCurrentWorkspace; // override any useCurrentWorkspaceId mock for this set of tests
    const consoleError = console.error; // testing for errors in these tests, so we need to silence them
    beforeAll(() => {
      existingWorkspaceMock = mockUseCurrentWorkspace.getMockImplementation() as typeof mockUseCurrentWorkspace;
      mockUseCurrentWorkspace.mockImplementation(() => mockWorkspace);
      console.error = () => void 0;
    });
    afterAll(() => {
      mockUseCurrentWorkspace.mockImplementation(existingWorkspaceMock);
      console.error = consoleError;
    });

    it("throws an error when instance query includes a resourceId", () => {
      mockUseListPermissions.mockImplementation(() => ({
        permissions: [{ permissionType: "instance_admin" }],
      }));

      expect(() =>
        renderHook(() => useRbac({ resourceType: "INSTANCE", role: "ADMIN", resourceId: "some-workspace" }))
      ).toThrowError("Invalid RBAC query: resource INSTANCE with resourceId some-workspace");
    });

    it("throws an error when non-instance query is missing and cannot find a resourceId", () => {
      mockUseListPermissions.mockImplementation(() => ({
        permissions: [{ permissionType: "instance_admin" }],
      }));

      expect(() => renderHook(() => useRbac({ resourceType: "ORGANIZATION", role: "ADMIN" }))).toThrowError(
        "Invalid RBAC query: resource ORGANIZATION with resourceId undefined"
      );

      mockUseListPermissions.mockImplementation(() => ({
        permissions: [{ permissionType: "instance_admin" }],
      }));
    });
  });
});
