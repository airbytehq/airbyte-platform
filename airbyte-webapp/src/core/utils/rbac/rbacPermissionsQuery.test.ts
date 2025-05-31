import { mockWebappConfig } from "test-utils/mock-data/mockWebappConfig";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";

import { RbacPermission, RbacQuery, partitionPermissionType, useRbacPermissionsQuery } from "./rbacPermissionsQuery";

jest.mock("core/api", () => ({
  useGetWorkspace: jest.fn((workspaceId: string) => {
    const workspace = { ...mockWorkspace };

    if (workspaceId === "test-workspace") {
      workspace.organizationId = "org-with-test-workspace";
    } else if (workspaceId === "workspace-1" || workspaceId === "workspace-2") {
      workspace.organizationId = "org-with-two-workspaces";
    }

    return workspace;
  }),
  getWebappConfig: () => mockWebappConfig,
}));

describe("partitionPermissionType", () => {
  it("correctly parses permissions", () => {
    expect(partitionPermissionType("instance_admin")).toEqual(["INSTANCE", "ADMIN"]);

    expect(partitionPermissionType("organization_admin")).toEqual(["ORGANIZATION", "ADMIN"]);
    expect(partitionPermissionType("organization_reader")).toEqual(["ORGANIZATION", "READER"]);

    expect(partitionPermissionType("workspace_admin")).toEqual(["WORKSPACE", "ADMIN"]);
    expect(partitionPermissionType("workspace_reader")).toEqual(["WORKSPACE", "READER"]);
  });

  it("maps workspace_owner to workspace_admin", () => {
    expect(partitionPermissionType("workspace_owner")).toEqual(["WORKSPACE", "ADMIN"]);
  });
});

describe("useRbacPermissionsQuery", () => {
  describe("permission grants", () => {
    // title, query, permissions, expectedResult
    it.each<[string, RbacQuery, RbacPermission[], boolean | null]>([
      /* NO PERMISSIONS */
      [
        "no permission grants no access to workspace",
        { resourceType: "WORKSPACE", resourceId: "test-workspace", role: "READER" },
        [],
        false,
      ],
      [
        "no permission grants no access to organization",
        { resourceType: "ORGANIZATION", resourceId: "test-workspace", role: "READER" },
        [],
        false,
      ],
      [
        "no permission grants no access to instance",
        { resourceType: "INSTANCE", role: "ADMIN", resourceId: "" },
        [],
        false,
      ],

      /* BASIC WORKSPACE PERMISSIONS */
      [
        "workspace_reader permission grants access to the workspace",
        { resourceType: "WORKSPACE", resourceId: "test-workspace", role: "READER" },
        [{ permissionType: "workspace_reader", workspaceId: "test-workspace" }],
        true,
      ],
      [
        "workspace_admin permission grants access to the workspace",
        { resourceType: "WORKSPACE", resourceId: "test-workspace", role: "READER" },
        [{ permissionType: "workspace_admin", workspaceId: "test-workspace" }],
        true,
      ],
      [
        "workspace_admin permission on another workspace does not grant access to the workspace",
        { resourceType: "WORKSPACE", resourceId: "test-workspace", role: "READER" },
        [{ permissionType: "workspace_admin", workspaceId: "an-whole-other-workspace" }],
        false,
      ],
      [
        "workspace_reader permission on a workspace does not satisfy workspace_admin permission on the same workspace",
        { resourceType: "WORKSPACE", resourceId: "test-workspace", role: "ADMIN" },
        [{ permissionType: "workspace_reader", workspaceId: "test-workspace" }],
        false,
      ],
      [
        "workspace_reader AND workspace_admin permission on a workspace does satisfy workspace_admin permission on the same workspace",
        { resourceType: "WORKSPACE", resourceId: "test-workspace", role: "ADMIN" },
        [
          { permissionType: "workspace_reader", workspaceId: "test-workspace" },
          { permissionType: "workspace_admin", workspaceId: "test-workspace" },
        ],
        true,
      ],

      /* BASIC ORGANIZATION PERMISSIONS */
      [
        "organization_reader permission grants access to the organization",
        { resourceType: "ORGANIZATION", resourceId: "test-organization", role: "READER" },
        [{ permissionType: "organization_reader", organizationId: "test-organization" }],
        true,
      ],
      [
        "organization_admin permission grants access to the organization",
        { resourceType: "ORGANIZATION", resourceId: "test-organization", role: "READER" },
        [{ permissionType: "organization_admin", organizationId: "test-organization" }],
        true,
      ],
      [
        "organization_admin permission on another organization does not grant access to the organization",
        { resourceType: "ORGANIZATION", resourceId: "test-organization", role: "READER" },
        [{ permissionType: "organization_admin", organizationId: "an-whole-other-organization" }],
        false,
      ],
      [
        "organization_reader permission on a organization does not satisfy organization_admin permission on the same organization",
        { resourceType: "ORGANIZATION", resourceId: "test-organization", role: "ADMIN" },
        [{ permissionType: "organization_reader", organizationId: "test-organization" }],
        false,
      ],
      [
        "organization_reader AND organization_admin permission on a organization does satisfy organization_admin permission on the same organization",
        { resourceType: "ORGANIZATION", resourceId: "test-organization", role: "ADMIN" },
        [
          { permissionType: "organization_reader", organizationId: "test-organization" },
          { permissionType: "organization_admin", organizationId: "test-organization" },
        ],
        true,
      ],

      /* ORGANIZATION PERMISSIONS INFORMING WORKSPACES */
      [
        "organization_editor permission grants access to read its workspace",
        { resourceType: "WORKSPACE", resourceId: "test-workspace", role: "READER" },
        [{ permissionType: "organization_editor", organizationId: "org-with-test-workspace" }],
        true,
      ],
      [
        "organization_admin permission does not grant access to read external workspaces",
        { resourceType: "WORKSPACE", resourceId: "test-workspace", role: "READER" },
        [{ permissionType: "organization_admin", organizationId: "org-with-two-workspaces" }],
        false,
      ],
      [
        "organization_admin permission does not grant access to read external workspaces but a workspace permission does",
        { resourceType: "WORKSPACE", resourceId: "test-workspace", role: "READER" },
        [
          { permissionType: "organization_admin", organizationId: "org-with-two-workspaces" },
          { permissionType: "workspace_editor", workspaceId: "test-workspace" },
        ],
        true,
      ],

      /* BASIC INSTANCE PERMISSIONS */
      [
        "instance_admin permission grants access to the instance",
        { resourceType: "INSTANCE", role: "ADMIN", resourceId: "" },
        [{ permissionType: "instance_admin" }],
        true,
      ],
      [
        "instance_admin permission grants access to an organization",
        { resourceType: "ORGANIZATION", resourceId: "test-organization", role: "ADMIN" },
        [{ permissionType: "instance_admin" }],
        true,
      ],
      [
        "instance_admin permission grants access to a workspace",
        { resourceType: "WORKSPACE", resourceId: "test-workspace", role: "ADMIN" },
        [{ permissionType: "instance_admin" }],
        true,
      ],
    ])("%s", (_title, query, permissions, expectedResult) => {
      expect(useRbacPermissionsQuery(permissions, [query])).toBe(expectedResult);
    });
  });

  describe("multiple queries", () => {
    it("returns true when the workspace permission is exact", () => {
      expect(
        useRbacPermissionsQuery(
          [
            { permissionType: "workspace_admin", workspaceId: "test-workspace" },
            { permissionType: "organization_reader", organizationId: "org-with-test-workspace" },
          ],
          [
            {
              resourceType: "WORKSPACE",
              role: "ADMIN",
              resourceId: "test-workspace",
            },
          ]
        )
      ).toBe(true);
    });

    it("returns true when the workspace permission is higher", () => {
      expect(
        useRbacPermissionsQuery(
          [
            { permissionType: "workspace_admin", workspaceId: "test-workspace" },
            { permissionType: "organization_reader", organizationId: "org-with-test-workspace" },
          ],
          [
            {
              resourceType: "WORKSPACE",
              role: "EDITOR",
              resourceId: "test-workspace",
            },
          ]
        )
      ).toBe(true);
    });

    it("returns true when the organization permission is exact", () => {
      expect(
        useRbacPermissionsQuery(
          [
            { permissionType: "workspace_reader", workspaceId: "test-workspace" },
            { permissionType: "organization_editor", organizationId: "org-with-test-workspace" },
          ],
          [
            {
              resourceType: "ORGANIZATION",
              role: "EDITOR",
              resourceId: "org-with-test-workspace",
            },
          ]
        )
      ).toBe(true);
    });

    it("returns true when the organization permission is higher", () => {
      expect(
        useRbacPermissionsQuery(
          [
            { permissionType: "workspace_reader", workspaceId: "test-workspace" },
            { permissionType: "organization_admin", organizationId: "org-with-test-workspace" },
          ],
          [
            {
              resourceType: "ORGANIZATION",
              role: "EDITOR",
              resourceId: "org-with-test-workspace",
            },
          ]
        )
      ).toBe(true);
    });

    it("returns false when the permissions do not match", () => {
      expect(
        useRbacPermissionsQuery(
          [
            { permissionType: "workspace_reader", workspaceId: "test-workspace" },
            { permissionType: "organization_editor", organizationId: "org-with-test-workspace" },
          ],
          [
            {
              resourceType: "WORKSPACE",
              role: "ADMIN",
              resourceId: "org-with-test-workspace",
            },
          ]
        )
      ).toBe(false);
    });
  });

  describe("degenerate cases", () => {
    it("returns false when an organization or workspace resource permission is missing an id", () => {
      expect(
        useRbacPermissionsQuery(
          [{ permissionType: "organization_reader" }],
          [
            {
              resourceType: "INSTANCE",
              role: "ADMIN",
              resourceId: "",
            },
          ]
        )
      ).toBe(false);

      expect(
        useRbacPermissionsQuery(
          [{ permissionType: "workspace_reader" }],
          [
            {
              resourceType: "INSTANCE",
              role: "ADMIN",
              resourceId: "",
            },
          ]
        )
      ).toBe(false);

      expect(
        useRbacPermissionsQuery(
          [{ permissionType: "organization_admin", organizationId: "test-organization" }],
          [
            {
              resourceType: "WORKSPACE",
              role: "EDITOR",
              resourceId: "",
            },
          ]
        )
      ).toBe(false);
    });
  });
});
