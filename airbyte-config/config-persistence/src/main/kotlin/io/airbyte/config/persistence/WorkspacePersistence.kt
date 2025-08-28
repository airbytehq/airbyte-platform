/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.config.Permission
import io.airbyte.config.StandardWorkspace
import io.airbyte.data.services.impls.jooq.DbConverter.buildStandardWorkspace
import io.airbyte.data.services.shared.ResourcesByOrganizationQueryPaginated
import io.airbyte.data.services.shared.ResourcesByUserQueryPaginated
import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.instance.configs.jooq.generated.Tables
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.Result
import org.jooq.impl.DSL
import java.io.IOException
import java.util.Locale
import java.util.Optional
import java.util.UUID

/**
 * Persistence Interface for Workspace table.
 */
class WorkspacePersistence(
  database: Database?,
) {
  private val database = ExceptionWrappingDatabase(database)

  /**
   * List all workspaces as user has instance_admin role. Returning result ordered by workspace name.
   * Supports pagination and keyword search.
   */
  @Throws(IOException::class)
  fun listWorkspacesByInstanceAdminUserPaginated(
    includeDeleted: Boolean,
    pageSize: Int,
    rowOffset: Int,
    keyword: Optional<String>,
  ): List<StandardWorkspace> =
    database
      .query<Result<Record>> { ctx: DSLContext ->
        ctx
          .select(Tables.WORKSPACE.asterisk())
          .from(Tables.WORKSPACE)
          .where(if (keyword.isPresent) Tables.WORKSPACE.NAME.containsIgnoreCase(keyword.get()) else DSL.noCondition())
          .and(
            if (includeDeleted) {
              DSL.noCondition()
            } else {
              Tables.WORKSPACE.TOMBSTONE.notEqual(
                true,
              )
            },
          ).orderBy(Tables.WORKSPACE.NAME.asc())
          .limit(pageSize)
          .offset(rowOffset)
          .fetch()
      }.stream()
      .map { buildStandardWorkspace(it) }
      .toList()

  /**
   * List all workspaces as user has instance_admin role. Returning result ordered by workspace name.
   * Supports keyword search.
   */
  @Throws(IOException::class)
  fun listWorkspacesByInstanceAdminUser(
    includeDeleted: Boolean,
    keyword: Optional<String>,
  ): List<StandardWorkspace> =
    database
      .query<Result<Record>> { ctx: DSLContext ->
        ctx
          .select(Tables.WORKSPACE.asterisk())
          .from(Tables.WORKSPACE)
          .where(if (keyword.isPresent) Tables.WORKSPACE.NAME.containsIgnoreCase(keyword.get()) else DSL.noCondition())
          .and(
            if (includeDeleted) {
              DSL.noCondition()
            } else {
              Tables.WORKSPACE.TOMBSTONE.notEqual(
                true,
              )
            },
          ).orderBy(Tables.WORKSPACE.NAME.asc())
          .fetch()
      }.stream()
      .map { buildStandardWorkspace(it) }
      .toList()

  /**
   * List all workspaces owned by org id, returning result ordered by workspace name. Supports
   * pagination and keyword search.
   */
  @Throws(IOException::class)
  fun listWorkspacesByOrganizationIdPaginated(
    query: ResourcesByOrganizationQueryPaginated,
    keyword: Optional<String>,
  ): List<StandardWorkspace> =
    database
      .query<Result<Record>> { ctx: DSLContext ->
        ctx
          .select(Tables.WORKSPACE.asterisk())
          .from(Tables.WORKSPACE)
          .where(Tables.WORKSPACE.ORGANIZATION_ID.eq(query.organizationId))
          .and(if (keyword.isPresent) Tables.WORKSPACE.NAME.containsIgnoreCase(keyword.get()) else DSL.noCondition())
          .and(
            if (query.includeDeleted) {
              DSL.noCondition()
            } else {
              Tables.WORKSPACE.TOMBSTONE.notEqual(
                true,
              )
            },
          ).orderBy(Tables.WORKSPACE.NAME.asc())
          .limit(query.pageSize)
          .offset(query.rowOffset)
          .fetch()
      }.stream()
      .map { buildStandardWorkspace(it) }
      .toList()

  /**
   * List all workspaces owned by org id, returning result ordered by workspace name. Supports keyword
   * search.
   */
  @Throws(IOException::class)
  fun listWorkspacesByOrganizationId(
    organizationId: UUID?,
    includeDeleted: Boolean,
    keyword: Optional<String>,
  ): List<StandardWorkspace> =
    database
      .query<Result<Record>> { ctx: DSLContext ->
        ctx
          .select(Tables.WORKSPACE.asterisk())
          .from(Tables.WORKSPACE)
          .where(Tables.WORKSPACE.ORGANIZATION_ID.eq(organizationId))
          .and(if (keyword.isPresent) Tables.WORKSPACE.NAME.containsIgnoreCase(keyword.get()) else DSL.noCondition())
          .and(
            if (includeDeleted) {
              DSL.noCondition()
            } else {
              Tables.WORKSPACE.TOMBSTONE.notEqual(
                true,
              )
            },
          ).orderBy(Tables.WORKSPACE.NAME.asc())
          .fetch()
      }.stream()
      .map { buildStandardWorkspace(it) }
      .toList()

  /**
   * Get search keyword with flexible matching.
   */
  private fun getSearchKeyword(keyword: Optional<String>): String =
    if (keyword.isPresent) {
      "%" + keyword.get().lowercase(Locale.getDefault()) + "%"
    } else {
      "%%"
    }

  /**
   * List all active workspaces readable by user id, returning result ordered by workspace name.
   * Supports keyword search.
   */
  @Throws(IOException::class)
  fun listActiveWorkspacesByUserId(
    userId: UUID?,
    keyword: Optional<String>,
  ): List<StandardWorkspace> {
    val searchKeyword = getSearchKeyword(keyword)
    return database
      .query<Result<Record>> { ctx: DSLContext ->
        ctx.fetch(
          PermissionPersistenceHelper.LIST_ACTIVE_WORKSPACES_BY_USER_ID_AND_PERMISSION_TYPES_QUERY,
          userId,
          PermissionPersistenceHelper.getGrantingPermissionTypeArray(Permission.PermissionType.WORKSPACE_READER),
          searchKeyword,
        )
      }.stream()
      .map { buildStandardWorkspace(it) }
      .toList()
  }

  /**
   * List all workspaces readable by user id, returning result ordered by workspace name. Supports
   * pagination and keyword search.
   */
  @Throws(IOException::class)
  fun listWorkspacesByUserIdPaginated(
    query: ResourcesByUserQueryPaginated,
    keyword: Optional<String>,
  ): List<StandardWorkspace> {
    val searchKeyword = getSearchKeyword(keyword)
    val workspaceQuery = (
      PermissionPersistenceHelper.LIST_ACTIVE_WORKSPACES_BY_USER_ID_AND_PERMISSION_TYPES_QUERY +
        " LIMIT {3}" +
        " OFFSET {4}"
    )

    return database
      .query<Result<Record>> { ctx: DSLContext ->
        ctx.fetch(
          workspaceQuery,
          query.userId,
          PermissionPersistenceHelper.getGrantingPermissionTypeArray(Permission.PermissionType.WORKSPACE_READER),
          searchKeyword,
          query.pageSize,
          query.rowOffset,
        )
      }.stream()
      .map { buildStandardWorkspace(it) }
      .toList()
  }

  /**
   * Fetch the oldest, non-tombstoned Workspace that belongs to the given Organization.
   */
  @Throws(IOException::class)
  fun getDefaultWorkspaceForOrganization(organizationId: UUID): StandardWorkspace =
    database
      .query<Result<Record>> { ctx: DSLContext ->
        ctx
          .select(Tables.WORKSPACE.asterisk())
          .from(Tables.WORKSPACE)
          .where(Tables.WORKSPACE.ORGANIZATION_ID.eq(organizationId))
          .and(Tables.WORKSPACE.TOMBSTONE.notEqual(true))
          .orderBy(Tables.WORKSPACE.CREATED_AT.asc())
          .limit(1)
          .fetch()
      }.stream()
      .map { buildStandardWorkspace(it) }
      .findFirst()
      .orElseThrow { RuntimeException("No workspace found for organization: $organizationId") }

  /**
   * Check if any workspace exists with initialSetupComplete: true, tombstoned or not.
   */
  @Throws(IOException::class)
  fun getInitialSetupComplete(): Boolean =
    database.query { ctx: DSLContext ->
      ctx.fetchExists(
        Tables.WORKSPACE,
        Tables.WORKSPACE.INITIAL_SETUP_COMPLETE.eq(true),
      )
    }

  companion object {
    const val DEFAULT_WORKSPACE_NAME: String = "Default Workspace"
  }
}
