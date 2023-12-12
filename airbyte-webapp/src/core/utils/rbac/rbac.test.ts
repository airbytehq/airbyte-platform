import { renderHook } from "@testing-library/react";

import { mockUser } from "test-utils/mock-data/mockUser";

import { useListPermissions } from "core/api";
import { PermissionRead } from "core/api/types/AirbyteClient";

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
  };
});

const mockUseListPermissions = useListPermissions as unknown as jest.Mock<{ permissions: RbacPermission[] }>;

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
      expect(mockUseRbacPermissionsQuery.mock.lastCall?.[1]).toEqual({
        resourceType: "INSTANCE",
        role: "ADMIN",
      });
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
    const consoleError = console.error; // testing for errors in these tests, so we need to silence them
    beforeAll(() => {
      console.error = () => void 0;
    });
    afterAll(() => {
      console.error = consoleError;
    });

    it("throws an error when instance query includes a resourceId", () => {
      mockUseListPermissions.mockImplementation(() => ({
        permissions: [{ permissionType: "instance_admin" }],
      }));

      expect(() =>
        renderHook(() => useRbac({ resourceType: "INSTANCE", role: "ADMIN", resourceId: "some-workspace" }))
      ).toThrow("Invalid RBAC query: resource INSTANCE with resourceId some-workspace");

      mockUseListPermissions.mockImplementation(() => ({
        permissions: [{ permissionType: "instance_admin" }],
      }));
    });
    // TODO: Update test to throw once cloud workspaces are migrated to organizations + rbac.ts invariant is adjusted to require organization id

    it("throws an error when an workspaceId is missing", () => {
      mockUseListPermissions.mockImplementation(() => ({
        permissions: [{ permissionType: "instance_admin" }],
      }));

      mockUseRbacPermissionsQuery.mockClear();
      expect(() => renderHook(() => useRbac({ resourceType: "WORKSPACE", role: "ADMIN" }))).toThrow(
        "Invalid RBAC query: resource WORKSPACE with resourceId undefined"
      );
    });

    it("does not throw an error when an organizationId is missing", () => {
      mockUseListPermissions.mockImplementation(() => ({
        permissions: [{ permissionType: "instance_admin" }],
      }));

      mockUseRbacPermissionsQuery.mockClear();
      renderHook(() => useRbac({ resourceType: "ORGANIZATION", role: "ADMIN" }));

      expect(mockUseRbacPermissionsQuery).toHaveBeenCalledTimes(1);
      expect(mockUseRbacPermissionsQuery.mock.lastCall?.[1]).toEqual({
        resourceType: "ORGANIZATION",
        role: "ADMIN",
        resourceId: undefined,
      });

      mockUseListPermissions.mockImplementation(() => ({
        permissions: [{ permissionType: "instance_admin" }],
      }));
    });
  });
});
