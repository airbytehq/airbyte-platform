/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.WORKSPACE;
import static org.jooq.impl.DSL.noCondition;

import io.airbyte.config.StandardWorkspace;
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
   * This query is to list all workspaces that a user has read permissions.
   */
  private final String listWorkspacesByUserIdBasicQuery =
      "WITH userOrgs AS (SELECT organization_id FROM permission WHERE user_id = {0}),"
          + " userWorkspaces AS ("
          + " SELECT workspace.id AS workspace_id FROM userOrgs JOIN workspace"
          + " ON workspace.organization_id = userOrgs.organization_id"
          + " UNION"
          + " SELECT workspace_id FROM permission WHERE user_id = {1}"
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
   * List all workspaces owned by user id, returning result ordered by workspace name. Supports
   * keyword search.
   */
  public List<StandardWorkspace> listWorkspacesByUserId(UUID userId, boolean includeDeleted, Optional<String> keyword)
      throws IOException {
    final String searchKeyword = getSearchKeyword(keyword);
    return database.query(ctx -> ctx.fetch(listWorkspacesByUserIdBasicQuery, userId, userId, searchKeyword))
        .stream()
        .map(DbConverter::buildStandardWorkspace)
        .toList();
  }

  /**
   * List all workspaces owned by user id, returning result ordered by workspace name. Supports
   * pagination and keyword search.
   */
  public List<StandardWorkspace> listWorkspacesByUserIdPaginated(final ResourcesByUserQueryPaginated query, Optional<String> keyword)
      throws IOException {
    final String searchKeyword = getSearchKeyword(keyword);
    final String workspaceQuery = listWorkspacesByUserIdBasicQuery
        + " LIMIT {3}"
        + " OFFSET {4}";

    return database.query(ctx -> ctx.fetch(workspaceQuery, query.userId(), query.userId(), searchKeyword, query.pageSize(), query.rowOffset()))
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
