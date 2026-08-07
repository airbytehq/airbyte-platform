import { useGetWorkspace } from "core/api";
import { PermissionRead, PermissionType } from "core/api/types/AirbyteClient";

export const RbacResourceHierarchy = ["INSTANCE", "ORGANIZATION", "WORKSPACE"] as const;
// SOURCE_EDITOR and DESTINATION_EDITOR sit between EDITOR and RUNNER: each can do everything a
// runner can, but neither is a full editor. Their order relative to each other is arbitrary and
// unobservable, because no RbacQuery is ever built with either role — `intentToRbacQuery` only
// targets ADMIN/EDITOR/RUNNER/READER/MEMBER, and the generated-intent path (useGeneratedIntent)
// matches permission types exactly instead of walking this hierarchy.
export const RbacRoleHierarchy = [
  "ADMIN",
  "EDITOR",
  "SOURCE_EDITOR",
  "DESTINATION_EDITOR",
  "RUNNER",
  "READER",
  "MEMBER",
] as const;
export type RbacResource = (typeof RbacResourceHierarchy)[number];
export type RbacRole = (typeof RbacRoleHierarchy)[number];

export interface RbacQuery {
  resourceType: RbacResource;
  resourceId?: string;
  role: RbacRole;
}
export type RbacQueryWithoutResourceId = Omit<RbacQuery, "resourceId">;

// allows for easier object creation as we want to align with PermissionRead but have no use for permissionId or userId when processing permissions
export type RbacPermission = Omit<PermissionRead, "permissionId" | "userId">;

/**
 * The resource and role each permission type maps onto. Spelled out rather than derived by
 * splitting the permission type on "_": `workspace_source_editor` has three parts, so splitting
 * yields the role "SOURCE", which is not in RbacRoleHierarchy. `indexOf` would then return -1 and
 * satisfy every role comparison in useRbacPermissionsQuery, making the role read as more privileged
 * than admin. The Record is keyed on PermissionType so adding a new one is a compile error here.
 */
const rbacPartsByPermissionType: Record<PermissionType, [RbacResource, RbacRole]> = {
  instance_admin: ["INSTANCE", "ADMIN"],
  organization_admin: ["ORGANIZATION", "ADMIN"],
  organization_editor: ["ORGANIZATION", "EDITOR"],
  organization_runner: ["ORGANIZATION", "RUNNER"],
  organization_reader: ["ORGANIZATION", "READER"],
  organization_member: ["ORGANIZATION", "MEMBER"],
  // for legacy support, workspace_owner maps to workspace_admin
  workspace_owner: ["WORKSPACE", "ADMIN"],
  workspace_admin: ["WORKSPACE", "ADMIN"],
  workspace_editor: ["WORKSPACE", "EDITOR"],
  workspace_source_editor: ["WORKSPACE", "SOURCE_EDITOR"],
  workspace_destination_editor: ["WORKSPACE", "DESTINATION_EDITOR"],
  workspace_runner: ["WORKSPACE", "RUNNER"],
  workspace_reader: ["WORKSPACE", "READER"],
};

/**
 * Accepts a permission type and returns its resource and role parts
 */
export const partitionPermissionType = (permissionType: RbacPermission["permissionType"]): [RbacResource, RbacRole] => {
  // instance_reader is a frontend-only role that is not part of PermissionType — see useGeneratedIntent.
  if ((permissionType as string) === "instance_reader") {
    return ["INSTANCE", "READER"];
  }

  return rbacPartsByPermissionType[permissionType];
};

/**
 * Don't call this outside of `core/utils/rbac`. Always use the `useRbac()` or (better) `useIntent()` hook instead.
 */
export const useRbacPermissionsQuery = (permissions: RbacPermission[], queries: RbacQuery[]) => {
  // to satisfy React's rule of hooks, we have to isolate a singiular workspace ID
  // from the queries; this is fine, because the queries list is built up from intents
  // which are applied to at most a single workspace
  const queriedWorkspaceIdPermissions = queries.filter((query) => query.resourceType === "WORKSPACE");
  const queriedWorkspaceIds = queriedWorkspaceIdPermissions
    .map((query) => query.resourceId)
    .reduce((acc, item) => {
      if (item) {
        acc.add(item);
      }
      return acc;
    }, new Set<string>());

  if (queriedWorkspaceIds.size > 1) {
    throw new Error(
      `Invalid RBAC query: Queries for multiple workspace IDs: ${Array.from(queriedWorkspaceIds).join(", ")}`
    );
  }

  const queriedWorkspaceId = queriedWorkspaceIds.size === 1 ? Array.from(queriedWorkspaceIds)[0] : undefined;

  const owningOrganizationId = useGetWorkspace(queriedWorkspaceId ?? "", {
    enabled: !!queriedWorkspaceId,
  })?.organizationId;

  return queries.some((query) => {
    const queryRoleHierarchy = RbacRoleHierarchy.indexOf(query.role);
    const queryResourceHierarchy = RbacResourceHierarchy.indexOf(query.resourceType);

    return permissions.some((permission) => {
      const [permissionResource, permissionRole] = partitionPermissionType(permission.permissionType);

      const permissionRoleHierarchy = RbacRoleHierarchy.indexOf(permissionRole);
      const permissionResourceHierarchy = RbacResourceHierarchy.indexOf(permissionResource);

      const { organizationId, workspaceId } = permission;

      if (query.resourceType === "WORKSPACE") {
        if (workspaceId && query.resourceId !== workspaceId) {
          // workspace permission applies to a different workspace
          return false;
        }

        // is this permission for an organization
        if (organizationId) {
          if (!query.resourceId) {
            return false;
          }

          if (owningOrganizationId !== organizationId) {
            // this organization permission does not apply to the workspace request
            return false;
          }
        }
      }

      if (query.resourceType === "ORGANIZATION") {
        if (organizationId && query.resourceId !== organizationId) {
          // organization permission applies to a different organization
          return false;
        }
      }

      return permissionRoleHierarchy <= queryRoleHierarchy && permissionResourceHierarchy <= queryResourceHierarchy;
    });
  });
};
