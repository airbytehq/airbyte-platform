/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.WORKSPACE;
import static org.jooq.impl.DSL.noCondition;

import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.helpers.PermissionHelper;
import io.airbyte.config.persistence.ConfigRepository.ResourcesByOrganizationQueryPaginated;
import io.airbyte.config.persistence.ConfigRepository.ResourcesByUserQueryPaginated;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * Persistence Interface for Workspace table.
 */
@Slf4j
public class WorkspacePersistence {

  private final ExceptionWrappingDatabase database;

  public WorkspacePersistence(final Database database) {
    this.database = new ExceptionWrappingDatabase(database);
  }

  /**
   * List all workspaces as user has instance_admin role. Returning result ordered by workspace name.
   * Supports pagination and keyword search.
   */
  public List<StandardWorkspace> listWorkspacesByInstanceAdminUserPaginated(final boolean includeDeleted,
                                                                            final int pageSize,
                                                                            final int rowOffset,
                                                                            Optional<String> keyword)
      throws IOException {
    return database.query(ctx -> ctx.select(WORKSPACE.asterisk())
        .from(WORKSPACE)
        .where(keyword.isPresent() ? WORKSPACE.NAME.containsIgnoreCase(keyword.get()) : noCondition())
        .and(includeDeleted ? noCondition() : WORKSPACE.TOMBSTONE.notEqual(true))
        .orderBy(WORKSPACE.NAME.asc())
        .limit(pageSize)
        .offset(rowOffset)
        .fetch())
        .stream()
        .map(DbConverter::buildStandardWorkspace)
        .toList();
  }

  /**
   * List all workspaces as user has instance_admin role. Returning result ordered by workspace name.
   * Supports keyword search.
   */
  public List<StandardWorkspace> listWorkspacesByInstanceAdminUser(final boolean includeDeleted, Optional<String> keyword)
      throws IOException {
    return database.query(ctx -> ctx.select(WORKSPACE.asterisk())
        .from(WORKSPACE)
        .where(keyword.isPresent() ? WORKSPACE.NAME.containsIgnoreCase(keyword.get()) : noCondition())
        .and(includeDeleted ? noCondition() : WORKSPACE.TOMBSTONE.notEqual(true))
        .orderBy(WORKSPACE.NAME.asc())
        .fetch())
        .stream()
        .map(DbConverter::buildStandardWorkspace)
        .toList();
  }

  /**
   * List all workspaces owned by org id, returning result ordered by workspace name. Supports
   * pagination and keyword search.
   */
  public List<StandardWorkspace> listWorkspacesByOrganizationIdPaginated(final ResourcesByOrganizationQueryPaginated query, Optional<String> keyword)
      throws IOException {
    return database.query(ctx -> ctx.select(WORKSPACE.asterisk())
        .from(WORKSPACE)
        .where(WORKSPACE.ORGANIZATION_ID.eq(query.organizationId()))
        .and(keyword.isPresent() ? WORKSPACE.NAME.containsIgnoreCase(keyword.get()) : noCondition())
        .and(query.includeDeleted() ? noCondition() : WORKSPACE.TOMBSTONE.notEqual(true))
        .orderBy(WORKSPACE.NAME.asc())
        .limit(query.pageSize())
        .offset(query.rowOffset())
        .fetch())
        .stream()
        .map(DbConverter::buildStandardWorkspace)
        .toList();
  }

  /**
   * List all workspaces owned by org id, returning result ordered by workspace name. Supports keyword
   * search.
   */
  public List<StandardWorkspace> listWorkspacesByOrganizationId(UUID organizationId, boolean includeDeleted, Optional<String> keyword)
      throws IOException {
    return database.query(ctx -> ctx.select(WORKSPACE.asterisk())
        .from(WORKSPACE)
        .where(WORKSPACE.ORGANIZATION_ID.eq(organizationId))
        .and(keyword.isPresent() ? WORKSPACE.NAME.containsIgnoreCase(keyword.get()) : noCondition())
        .and(includeDeleted ? noCondition() : WORKSPACE.TOMBSTONE.notEqual(true))
        .orderBy(WORKSPACE.NAME.asc())
        .fetch())
        .stream()
        .map(DbConverter::buildStandardWorkspace)
        .toList();
  }

  /**
   * This query lists all workspaces that a particular user has the indicated permissions for. The
   * query is parameterized by a user id, a permission type array, and a keyword search string.
   * <p>
   * Note: The permission type array should include the valid set of permission types that can be used
   * to infer workspace access.
   * <p>
   * For instance, if the passed-in permission type array contains `organization_admin` and
   * `workspace_admin`, then the query will return all workspaces that belong to an organization that
   * the user has `organization_admin` permissions for, as well as all workspaces that the user has
   * `workspace_admin` permissions for.
   */
  private final String listWorkspacesByUserIdAndPermissionTypeBasicQuery =
      "WITH userOrgs AS (SELECT organization_id FROM permission WHERE user_id = {0} AND permission_type = ANY({1}::permission_type[])),"
          + " userWorkspaces AS ("
          + " SELECT workspace.id AS workspace_id FROM userOrgs JOIN workspace"
          + " ON workspace.organization_id = userOrgs.organization_id"
          + " UNION"
          + " SELECT workspace_id FROM permission WHERE user_id = {0} AND permission_type = ANY({1}::permission_type[])"
          + " )"
          + " SELECT * from workspace"
          + " WHERE workspace.id IN (SELECT workspace_id from userWorkspaces)"
          + " AND name ILIKE {2}"
          + " AND tombstone = false"
          + " ORDER BY name ASC";

  /**
   * Get search keyword with flexible matching.
   */
  private String getSearchKeyword(Optional<String> keyword) {
    if (keyword.isPresent()) {
      return "%" + keyword.get().toLowerCase() + "%";
    } else {
      return "%%";
    }
  }

  /**
   * Get an array of the Jooq enum values for the permission types that grant the target permission
   * type. Used for `ANY(?)` clauses in SQL queries.
   */
  private io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType[] getGrantingPermissionTypeArray(final PermissionType targetPermissionType) {
    return PermissionHelper.getPermissionTypesThatGrantTargetPermission(targetPermissionType)
        .stream()
        .map(this::convertConfigPermissionTypeToJooqPermissionType)
        .toList()
        .toArray(new io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType[0]);
  }

  private io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType convertConfigPermissionTypeToJooqPermissionType(final PermissionType permissionType) {
    // workspace owner is deprecated and doesn't exist in OSS jooq. it is equivalent to workspace admin.
    if (permissionType.equals(PermissionType.WORKSPACE_OWNER)) {
      return io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType.workspace_admin;
    }

    return io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType.valueOf(permissionType.value());
  }

  /**
   * List all workspaces readable by user id, returning result ordered by workspace name. Supports
   * keyword search.
   */
  public List<StandardWorkspace> listWorkspacesByUserId(UUID userId, Optional<String> keyword)
      throws IOException {
    final String searchKeyword = getSearchKeyword(keyword);
    return database
        .query(ctx -> ctx.fetch(listWorkspacesByUserIdAndPermissionTypeBasicQuery, userId,
            getGrantingPermissionTypeArray(PermissionType.WORKSPACE_READER), searchKeyword))
        .stream()
        .map(DbConverter::buildStandardWorkspace)
        .toList();
  }

  /**
   * List all workspaces readable by user id, returning result ordered by workspace name. Supports
   * pagination and keyword search.
   */
  public List<StandardWorkspace> listWorkspacesByUserIdPaginated(final ResourcesByUserQueryPaginated query, Optional<String> keyword)
      throws IOException {
    final String searchKeyword = getSearchKeyword(keyword);
    final String workspaceQuery = listWorkspacesByUserIdAndPermissionTypeBasicQuery
        + " LIMIT {3}"
        + " OFFSET {4}";

    return database
        .query(ctx -> ctx.fetch(workspaceQuery, query.userId(), getGrantingPermissionTypeArray(PermissionType.WORKSPACE_READER), searchKeyword,
            query.pageSize(), query.rowOffset()))
        .stream()
        .map(DbConverter::buildStandardWorkspace)
        .toList();
  }

  /**
   * Fetch the oldest, non-tombstoned Workspace that belongs to the given Organization.
   */
  public StandardWorkspace getDefaultWorkspaceForOrganization(final UUID organizationId) throws IOException {
    return database.query(ctx -> ctx.select(WORKSPACE.asterisk())
        .from(WORKSPACE)
        .where(WORKSPACE.ORGANIZATION_ID.eq(organizationId))
        .and(WORKSPACE.TOMBSTONE.notEqual(true))
        .orderBy(WORKSPACE.CREATED_AT.asc())
        .limit(1)
        .fetch())
        .stream()
        .map(DbConverter::buildStandardWorkspace)
        .findFirst()
        .orElseThrow(() -> new RuntimeException("No workspace found for organization: " + organizationId));
  }

}
