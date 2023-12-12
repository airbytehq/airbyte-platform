/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import io.airbyte.config.helpers.PermissionHelper;
import io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType;

public class PermissionPersistenceHelper {

  /**
   * Get an array of the Jooq enum values for the permission types that grant the target permission
   * type. Used for `ANY(?)` clauses in SQL queries.
   */
  public static PermissionType[] getGrantingPermissionTypeArray(final io.airbyte.config.Permission.PermissionType targetPermissionType) {
    return PermissionHelper.getPermissionTypesThatGrantTargetPermission(targetPermissionType)
        .stream()
        .map(PermissionPersistenceHelper::convertConfigPermissionTypeToJooqPermissionType)
        .toList()
        .toArray(new io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType[0]);
  }

  public static PermissionType convertConfigPermissionTypeToJooqPermissionType(final io.airbyte.config.Permission.PermissionType permissionType) {
    if (permissionType == null) {
      return null;
    }
    // workspace owner is deprecated and doesn't exist in OSS jooq. it is equivalent to workspace admin.
    if (permissionType.equals(io.airbyte.config.Permission.PermissionType.WORKSPACE_OWNER)) {
      return io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType.workspace_admin;
    }

    return io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType.valueOf(permissionType.value());
  }

  /**
   * This query lists all active workspaces that a particular user has the indicated permissions for.
   * The query is parameterized by a user id, a permission type array, and a keyword search string.
   * <p>
   * Note: The permission type array should include the valid set of permission types that can be used
   * to infer workspace access.
   * <p>
   * For instance, if the passed-in permission type array contains `organization_admin` and
   * `workspace_admin`, then the query will return all workspaces that belong to an organization that
   * the user has `organization_admin` permissions for, as well as all workspaces that the user has
   * `workspace_admin` permissions for.
   */
  public static final String LIST_ACTIVE_WORKSPACES_BY_USER_ID_AND_PERMISSION_TYPES_QUERY =
      "WITH "
          + " userOrgs AS ("
          + "   SELECT organization_id FROM permission WHERE user_id = {0} AND permission_type = ANY({1}::permission_type[])"
          + " ),"
          + " userWorkspaces AS ("
          + "   SELECT workspace.id AS workspace_id FROM userOrgs JOIN workspace"
          + "   ON workspace.organization_id = userOrgs.organization_id"
          + "   UNION"
          + "   SELECT workspace_id FROM permission WHERE user_id = {0} AND permission_type = ANY({1}::permission_type[])"
          + " )"
          + " SELECT * from workspace"
          + " WHERE workspace.id IN (SELECT workspace_id from userWorkspaces)"
          + " AND name ILIKE {2}"
          + " AND tombstone = false"
          + " ORDER BY name ASC";

  /**
   * This query lists all users that can access the particular workspace through possession of the
   * indicated permissions. The query is parameterized by a workspace id and a permission type array.
   * <p>
   * Note: The permission type array should include the valid set of permission types that can be used
   * to infer workspace access.
   * <p>
   * For instance, if the passed-in permission type array contains `organization_admin` and
   * `workspace_admin`, then the query will return all users that can access the indicated workspace
   * through possession of either of those two permission_types.
   */
  public static final String LIST_USERS_BY_WORKSPACE_ID_AND_PERMISSION_TYPES_QUERY =
      "WITH "
          + " orgWorkspaces AS ("
          + "  SELECT organization_id FROM workspace WHERE workspace.id = {0}"
          + " ),"
          + " usersInOrgWithPerm AS ("
          + "   SELECT permission.user_id FROM permission"
          + "   JOIN orgWorkspaces ON permission.organization_id = orgWorkspaces.organization_id"
          + "   WHERE permission_type = ANY({1}::permission_type[])"
          + " ),"
          + " usersInWorkspaceWithPerm AS ("
          + "   SELECT user_id FROM permission WHERE workspace_id = {0} "
          + "   AND permission_type = ANY({1}::permission_type[])"
          + " )"
          + " SELECT * from \"user\""
          + " WHERE \"user\".id IN (SELECT user_id FROM usersInOrgWithPerm UNION SELECT user_id FROM usersInWorkspaceWithPerm)"
          + " ORDER BY name ASC";

}
