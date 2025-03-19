/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
          + " userOrg AS ("
          + "   SELECT organization_id FROM permission WHERE user_id = {0} AND permission_type = ANY({1}::permission_type[])"
          + " ),"
          + " userWorkspaces AS ("
          + "   SELECT workspace.id AS workspace_id FROM userOrg JOIN workspace"
          + "   ON workspace.organization_id = userOrg.organization_id"
          + "   UNION"
          + "   SELECT workspace_id FROM permission WHERE user_id = {0} AND permission_type = ANY({1}::permission_type[])"
          + " )"
          + " SELECT * from workspace"
          + " WHERE workspace.id IN (SELECT workspace_id from userWorkspaces)"
          + " AND name ILIKE {2}"
          + " AND tombstone = false"
          + " ORDER BY name ASC";

  // The following constants are used to alias columns in the below query to avoid
  // ambiguity when joining the same table multiple times. They are public so that
  // callers can reference them when accessing the resulting records.
  public static final String WORKSPACE_PERMISSION_ID_ALIAS = "workspace_perm_id";
  public static final String WORKSPACE_PERMISSION_TYPE_ALIAS = "workspace_perm_type";
  public static final String WORKSPACE_PERMISSION_WORKSPACE_ID_ALIAS = "workspace_perm_workspace_id";
  public static final String ORG_PERMISSION_ID_ALIAS = "org_perm_id";
  public static final String ORG_PERMISSION_TYPE_ALIAS = "org_perm_type";
  public static final String ORG_PERMISSION_ORG_ID_ALIAS = "org_perm_org_id";

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
   * <p>
   * The resulting records include all columns from the user table, as well as columns from the
   * permission table for the workspace-level and/or organization-level permissions that grant the
   * user access to the indicated workspace.
   */
  public static final String LIST_USERS_BY_WORKSPACE_ID_AND_PERMISSION_TYPES_QUERY =
      "WITH "
          + " workspaceOrg AS ("
          + "  SELECT organization_id FROM workspace WHERE workspace.id = {0}"
          + " ),"
          + " usersInOrgWithPerm AS ("
          + "   SELECT permission.user_id,"
          + "          permission.organization_id AS " + ORG_PERMISSION_ORG_ID_ALIAS + ","
          + "          permission.id AS " + ORG_PERMISSION_ID_ALIAS + ","
          + "          permission.permission_type AS " + ORG_PERMISSION_TYPE_ALIAS
          + "   FROM permission"
          + "   JOIN workspaceOrg ON permission.organization_id = workspaceOrg.organization_id"
          + "   WHERE permission_type = ANY({1}::permission_type[])"
          + " ),"
          + " usersInWorkspaceWithPerm AS ("
          + "   SELECT permission.user_id,"
          + "          permission.workspace_id AS " + WORKSPACE_PERMISSION_WORKSPACE_ID_ALIAS + ","
          + "          permission.id AS " + WORKSPACE_PERMISSION_ID_ALIAS + ","
          + "          permission.permission_type AS " + WORKSPACE_PERMISSION_TYPE_ALIAS
          + "   FROM permission WHERE workspace_id = {0} "
          + "   AND permission_type = ANY({1}::permission_type[])"
          + " )"
          + " SELECT \"user\".*,"
          + "        uo." + ORG_PERMISSION_ID_ALIAS + ","
          + "        uo." + ORG_PERMISSION_TYPE_ALIAS + ","
          + "        uo." + ORG_PERMISSION_ORG_ID_ALIAS + ","
          + "        uw." + WORKSPACE_PERMISSION_ID_ALIAS + ","
          + "        uw." + WORKSPACE_PERMISSION_TYPE_ALIAS + ","
          + "        uw." + WORKSPACE_PERMISSION_WORKSPACE_ID_ALIAS
          + " FROM \"user\""
          + " LEFT JOIN usersInOrgWithPerm uo ON \"user\".id = uo.user_id"
          + " LEFT JOIN usersInWorkspaceWithPerm uw ON \"user\".id = uw.user_id"
          + " WHERE \"user\".id IN (SELECT user_id FROM usersInOrgWithPerm UNION SELECT user_id FROM usersInWorkspaceWithPerm)"
          + " ORDER BY name ASC";

}
