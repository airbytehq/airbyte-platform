import { useGetWorkspace } from "core/api";
import { PermissionRead } from "core/api/types/AirbyteClient";

export const RbacResourceHierarchy = ["INSTANCE", "ORGANIZATION", "WORKSPACE"] as const;
export const RbacRoleHierarchy = ["ADMIN", "EDITOR", "RUNNER", "READER", "MEMBER"] as const;
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
 * Accepts a permission type and splits it into its resource and role parts
 */
export const partitionPermissionType = (permissionType: RbacPermission["permissionType"]): [RbacResource, RbacRole] => {
  // for legacy support, map workspace_owner to workspace_adin
  if (permissionType === "workspace_owner") {
    permissionType = "workspace_admin";
  }

  // type guarantees all of the enumerations of PermissionType are handled
  type PermissionTypeParts = typeof permissionType extends `${infer Resource}_${infer Role}` ? [Resource, Role] : never;

  const [permissionResource, permissionRole] = permissionType.split("_") as PermissionTypeParts;
  const rbacResource: RbacResource = permissionResource.toUpperCase() as Uppercase<typeof permissionResource>;
  const rbacRole: RbacRole = permissionRole.toUpperCase() as Uppercase<typeof permissionRole>;
  return [rbacResource, rbacRole];
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
