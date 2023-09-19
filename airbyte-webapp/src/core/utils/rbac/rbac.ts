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

  // invariant check
  if ((!queryUsesResourceId && resourceId) || (queryUsesResourceId && !resourceId)) {
    // TODO: This is a patch to handle the fact that workspaces on cloud do not have an organization.
    if (resourceType === "ORGANIZATION") {
      console.log("Missing organization id");
    } else {
      // this will throw if there is a missing workspace id OR a resourceId is passed to an instance query
      throw new Error(`Invalid RBAC query: resource ${resourceType} with resourceId ${resourceId}`);
    }
  }

  // above invariant guarantees resourceId is defined when needed
  // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
  const assembledQuery = queryUsesResourceId ? { resourceType, resourceId: resourceId!, role } : { resourceType, role };

  return useRbacPermissionsQuery(permissions, assembledQuery);
};
