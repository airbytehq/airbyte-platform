/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.WORKSPACE;
import static org.jooq.impl.DSL.noCondition;

import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.persistence.ConfigRepository.ResourcesByOrganizationQueryPaginated;
import io.airbyte.config.persistence.ConfigRepository.ResourcesByUserQueryPaginated;
import io.airbyte.data.services.impls.jooq.DbConverter;
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

  public static final String DEFAULT_WORKSPACE_NAME = "Default Workspace";

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
                                                                            final Optional<String> keyword)
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
  public List<StandardWorkspace> listWorkspacesByInstanceAdminUser(final boolean includeDeleted, final Optional<String> keyword)
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
  public List<StandardWorkspace> listWorkspacesByOrganizationIdPaginated(final ResourcesByOrganizationQueryPaginated query,
                                                                         final Optional<String> keyword)
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
  public List<StandardWorkspace> listWorkspacesByOrganizationId(final UUID organizationId,
                                                                final boolean includeDeleted,
                                                                final Optional<String> keyword)
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
   * Get search keyword with flexible matching.
   */
  private String getSearchKeyword(final Optional<String> keyword) {
    if (keyword.isPresent()) {
      return "%" + keyword.get().toLowerCase() + "%";
    } else {
      return "%%";
    }
  }

  /**
   * List all active workspaces readable by user id, returning result ordered by workspace name.
   * Supports keyword search.
   */
  public List<StandardWorkspace> listActiveWorkspacesByUserId(final UUID userId, final Optional<String> keyword)
      throws IOException {
    final String searchKeyword = getSearchKeyword(keyword);
    return database
        .query(ctx -> ctx.fetch(
            PermissionPersistenceHelper.LIST_ACTIVE_WORKSPACES_BY_USER_ID_AND_PERMISSION_TYPES_QUERY,
            userId,
            PermissionPersistenceHelper.getGrantingPermissionTypeArray(PermissionType.WORKSPACE_READER),
            searchKeyword))
        .stream()
        .map(DbConverter::buildStandardWorkspace)
        .toList();
  }

  /**
   * List all workspaces readable by user id, returning result ordered by workspace name. Supports
   * pagination and keyword search.
   */
  public List<StandardWorkspace> listWorkspacesByUserIdPaginated(final ResourcesByUserQueryPaginated query, final Optional<String> keyword)
      throws IOException {
    final String searchKeyword = getSearchKeyword(keyword);
    final String workspaceQuery = PermissionPersistenceHelper.LIST_ACTIVE_WORKSPACES_BY_USER_ID_AND_PERMISSION_TYPES_QUERY
        + " LIMIT {3}"
        + " OFFSET {4}";

    return database
        .query(ctx -> ctx.fetch(
            workspaceQuery,
            query.userId(),
            PermissionPersistenceHelper.getGrantingPermissionTypeArray(PermissionType.WORKSPACE_READER),
            searchKeyword,
            query.pageSize(),
            query.rowOffset()))
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
