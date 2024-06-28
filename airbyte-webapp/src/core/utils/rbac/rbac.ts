import { useListPermissions } from "core/api";
import { useCurrentUser } from "core/services/auth";

import { RbacQuery, useRbacPermissionsQuery } from "./rbacPermissionsQuery";

/**
 * Takes a list of permissions and a full or partial query, returning a boolean representing if the user has any permissions satisfying the query
 */
export const useRbac = (queries: RbacQuery[]) => {
  const { userId } = useCurrentUser();
  const { permissions } = useListPermissions(userId);

  queries.forEach((query) => {
    const { resourceType } = query;
    const resourceId = "resourceId" in query ? query.resourceId : undefined;

    const queryUsesResourceId = resourceType !== "INSTANCE";

    if (queryUsesResourceId && !resourceId) {
      throw new Error(`Invalid RBAC query: Missing id for resource ${resourceType}`);
    }

    if (!queryUsesResourceId && resourceId) {
      throw new Error(`Invalid RBAC query: Passed resource id ${resourceId} for ${resourceType} query`);
    }
  });

  return useRbacPermissionsQuery(permissions, queries);
};
