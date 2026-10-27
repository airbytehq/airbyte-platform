/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
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
          .join(Tables.ORGANIZATION)
          .on(Tables.ORGANIZATION.ID.eq(Tables.WORKSPACE.ORGANIZATION_ID))
          .where(if (keyword.isPresent) Tables.WORKSPACE.NAME.containsIgnoreCase(keyword.get()) else DSL.noCondition())
          .and(
            if (includeDeleted) {
              DSL.noCondition()
            } else {
              Tables.WORKSPACE.TOMBSTONE
                .notEqual(
                  true,
                ).and(Tables.ORGANIZATION.TOMBSTONE.notEqual(true))
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
  fun listWorkspacesByInstanceAdminUser(
    includeDeleted: Boolean,
    keyword: Optional<String>,
  ): List<StandardWorkspace> =
    database
      .query<Result<Record>> { ctx: DSLContext ->
        ctx
          .select(Tables.WORKSPACE.asterisk())
          .from(Tables.WORKSPACE)
          .join(Tables.ORGANIZATION)
          .on(Tables.ORGANIZATION.ID.eq(Tables.WORKSPACE.ORGANIZATION_ID))
          .where(if (keyword.isPresent) Tables.WORKSPACE.NAME.containsIgnoreCase(keyword.get()) else DSL.noCondition())
          .and(
            if (includeDeleted) {
              DSL.noCondition()
            } else {
              Tables.WORKSPACE.TOMBSTONE
                .notEqual(
                  true,
                ).and(Tables.ORGANIZATION.TOMBSTONE.notEqual(true))
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
   * List all workspaces in an organization that a particular user has access to based on their permissions.
   * Uses similar logic to listActiveWorkspacesByUserId but filters by organization.
   * Returns result ordered by workspace name. Supports keyword search.
   */
  fun listWorkspacesInOrganizationByUserId(
    organizationId: UUID,
    userId: UUID,
    keyword: Optional<String>,
  ): List<StandardWorkspace> {
    val searchKeyword = getSearchKeyword(keyword)
    return database
      .query { ctx: DSLContext ->
        ctx.fetch(
          PermissionPersistenceHelper.LIST_WORKSPACES_IN_ORGANIZATION_BY_USER_ID_AND_PERMISSION_TYPES_QUERY,
          userId,
          PermissionPersistenceHelper.getGrantingPermissionTypeArray(Permission.PermissionType.WORKSPACE_READER),
          organizationId,
          searchKeyword,
        )
      }.stream()
      .map { buildStandardWorkspace(it) }
      .toList()
  }

  /**
   * List all workspaces in an organization that a particular user has access to based on their permissions.
   * Supports pagination and keyword search. Returns result ordered by workspace name.
   */
  fun listWorkspacesInOrganizationByUserIdPaginated(
    query: ResourcesByOrganizationQueryPaginated,
    userId: UUID,
    keyword: Optional<String>,
  ): List<StandardWorkspace> {
    val searchKeyword = getSearchKeyword(keyword)
    val workspaceQuery = (
      PermissionPersistenceHelper.LIST_WORKSPACES_IN_ORGANIZATION_BY_USER_ID_AND_PERMISSION_TYPES_QUERY +
        " LIMIT {4}" +
        " OFFSET {5}"
    )

    return database
      .query { ctx: DSLContext ->
        ctx.fetch(
          workspaceQuery,
          userId,
          PermissionPersistenceHelper.getGrantingPermissionTypeArray(Permission.PermissionType.WORKSPACE_READER),
          query.organizationId,
          searchKeyword,
          query.pageSize,
          query.rowOffset,
        )
      }.stream()
      .map { buildStandardWorkspace(it) }
      .toList()
  }

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

  /** Same effective membership query, returning no workspace configuration or credentials. */
  fun listActiveWorkspaceIdsByUserId(userId: UUID): List<UUID> =
    database.query { ctx: DSLContext ->
      ctx
        .fetch(
          "SELECT id FROM (${PermissionPersistenceHelper.LIST_ACTIVE_WORKSPACES_BY_USER_ID_AND_PERMISSION_TYPES_QUERY}) accessible_workspaces",
          userId,
          PermissionPersistenceHelper.getGrantingPermissionTypeArray(Permission.PermissionType.WORKSPACE_READER),
          getSearchKeyword(Optional.empty()),
        ).map { it.get("id", UUID::class.java) }
    }

  /**
   * List all workspaces readable by user id, returning result ordered by workspace name. Supports
   * pagination and keyword search.
   */
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
   * Check whether a workspace with the given ID exists, tombstoned or not.
   */
  fun workspaceExists(workspaceId: UUID): Boolean =
    database.query { ctx: DSLContext ->
      ctx.fetchExists(
        Tables.WORKSPACE,
        Tables.WORKSPACE.ID.eq(workspaceId),
      )
    }

  /**
   * Count the non-deleted workspaces owned by the given organization.
   */
  fun countWorkspacesByOrganizationId(organizationId: UUID): Long =
    database
      .query { ctx: DSLContext ->
        ctx
          .selectCount()
          .from(Tables.WORKSPACE)
          .where(Tables.WORKSPACE.ORGANIZATION_ID.eq(organizationId))
          .and(Tables.WORKSPACE.TOMBSTONE.notEqual(true))
          .fetchOne()!!
          .value1()
          .toLong()
      }

  /**
   * Tombstones a live workspace unless it is its organization's last one. The organization row is
   * locked for the check and the write, so concurrent deletes in the same organization serialize and
   * can't both remove the last two workspaces.
   */
  fun tombstoneWorkspace(
    workspaceId: UUID,
    organizationId: UUID,
    allowLastWorkspace: Boolean = false,
  ): WorkspaceTombstoneResult =
    database.transaction { ctx: DSLContext ->
      ctx
        .selectOne()
        .from(Tables.ORGANIZATION)
        .where(Tables.ORGANIZATION.ID.eq(organizationId))
        .forUpdate()
        .fetch()

      val liveWorkspaces =
        Tables.WORKSPACE.ORGANIZATION_ID
          .eq(organizationId)
          .and(Tables.WORKSPACE.TOMBSTONE.notEqual(true))
      when {
        !ctx.fetchExists(Tables.WORKSPACE, liveWorkspaces.and(Tables.WORKSPACE.ID.eq(workspaceId))) ->
          WorkspaceTombstoneResult.NOT_FOUND
        !allowLastWorkspace && ctx.fetchCount(Tables.WORKSPACE, liveWorkspaces) <= 1 ->
          WorkspaceTombstoneResult.LAST_WORKSPACE
        else -> {
          ctx
            .update(Tables.WORKSPACE)
            .set(Tables.WORKSPACE.TOMBSTONE, true)
            .set(Tables.WORKSPACE.UPDATED_AT, DSL.currentOffsetDateTime())
            .where(Tables.WORKSPACE.ID.eq(workspaceId))
            .execute()
          WorkspaceTombstoneResult.TOMBSTONED
        }
      }
    }

  /**
   * Check if any workspace exists with initialSetupComplete: true, tombstoned or not.
   */
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

enum class WorkspaceTombstoneResult {
  TOMBSTONED,

  /** The workspace is its organization's last live workspace. */
  LAST_WORKSPACE,

  /** The workspace does not exist, is already deleted, or belongs to another organization. */
  NOT_FOUND,
}
