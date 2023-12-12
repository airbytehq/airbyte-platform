import { useGetWorkspace } from "core/api";
import { PermissionRead } from "core/api/types/AirbyteClient";

export const RbacResourceHierarchy = ["INSTANCE", "ORGANIZATION", "WORKSPACE"] as const;
export const RbacRoleHierarchy = ["ADMIN", "EDITOR", "READER", "MEMBER"] as const;
export type RbacResource = (typeof RbacResourceHierarchy)[number];
export type RbacRole = (typeof RbacRoleHierarchy)[number];

export interface RbacQuery {
  resourceType: RbacResource;
  resourceId?: string;
  role: RbacRole;
}
export type RbacQueryWithoutResourceId = Omit<RbacQuery, "resourceId">; // to allow optionally reading `resourceId` from React context

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

export const useRbacPermissionsQuery = (permissions: RbacPermission[], query: RbacQuery) => {
  const queryRoleHierarchy = RbacRoleHierarchy.indexOf(query.role);
  const queryResourceHierarchy = RbacResourceHierarchy.indexOf(query.resourceType);

  const owningOrganizationId = useGetWorkspace(query.resourceId ?? "", {
    enabled: query.resourceType === "WORKSPACE" && !!query.resourceId,
  })?.organizationId;

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
};
