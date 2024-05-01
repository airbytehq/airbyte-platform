import { useListPermissions } from "core/api";
import { useCurrentUser } from "core/services/auth";

import { RbacQuery, RbacQueryWithoutResourceId, useRbacPermissionsQuery } from "./rbacPermissionsQuery";

/**
 * Takes a list of permissions and a full or partial query, returning a boolean representing if the user has any permissions satisfying the query
 */
export const useRbac = (query: RbacQuery | RbacQueryWithoutResourceId) => {
  const { resourceType, role } = query;
  const resourceId = "resourceId" in query ? query.resourceId : undefined;

  const queryUsesResourceId = resourceType !== "INSTANCE";

  const { userId } = useCurrentUser();
  const { permissions } = useListPermissions(userId);

  if (queryUsesResourceId && !resourceId) {
    throw new Error(`Invalid RBAC query: Missing id for resource ${resourceType}`);
  }

  if (!queryUsesResourceId && resourceId) {
    throw new Error(`Invalid RBAC query: Passed resource id ${resourceId} for ${resourceType} query`);
  }

  // above invariant guarantees resourceId is defined when needed
  const assembledQuery = queryUsesResourceId
    ? { resourceType, resourceId: resourceId ?? "", role }
    : { resourceType, role, resourceId: "" };

  return useRbacPermissionsQuery(permissions, assembledQuery);
};
