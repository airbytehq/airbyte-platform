/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.db.instance.configs.jooq.generated.enums.ScimResourceType
import io.micronaut.transaction.TransactionOperations
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.jooq.Condition
import org.jooq.DSLContext
import org.jooq.Field
import org.jooq.SQLDialect
import org.jooq.Table
import org.jooq.impl.DSL
import java.sql.Connection
import java.util.Locale
import java.util.UUID
import io.airbyte.db.instance.configs.jooq.generated.Tables as ConfigTables

/** Required, atomic DSR config cleanup. Workload cleanup runs separately and is best effort. */
@Singleton
class DsrConfigPersistence(
  @param:Named("config") private val transactions: TransactionOperations<Connection>,
  private val userPersistence: UserPersistence,
) {
  data class DeletionTargets(
    val userId: UUID,
    val workspaceIds: List<UUID>,
    val organizationIds: List<UUID>,
    val connectionIds: List<UUID>,
    val sourceIds: List<UUID>,
    val destinationIds: List<UUID>,
    val connectorBuilderProjectIds: List<UUID>,
    val authUserIds: List<String>,
  )

  fun delete(
    targets: DeletionTargets,
    datagrailId: String,
  ): ConfigDeletionCounts =
    transactions.executeWrite { transaction ->
      deleteRows(DSL.using(transaction.connection, SQLDialect.POSTGRES), targets, datagrailId)
    }

  private fun deleteRows(
    ctx: DSLContext,
    targets: DeletionTargets,
    datagrailId: String,
  ): ConfigDeletionCounts {
    ctx.execute("SET LOCAL statement_timeout = '1200s'")
    userPersistence.lockAuthUserReplacement(ctx, targets.userId)

    val scimMappingSnapshot =
      ctx
        .select(
          ConfigTables.SCIM_RESOURCE_MAPPING.ID,
          ConfigTables.SCIM_RESOURCE_MAPPING.ORGANIZATION_ID,
          ConfigTables.SCIM_RESOURCE_MAPPING.SCIM_CONFIGURATION_ID,
          ConfigTables.SCIM_RESOURCE_MAPPING.PRIMARY_EMAIL,
        ).from(ConfigTables.SCIM_RESOURCE_MAPPING)
        .where(ConfigTables.SCIM_RESOURCE_MAPPING.RESOURCE_TYPE.eq(ScimResourceType.USER))
        .and(ConfigTables.SCIM_RESOURCE_MAPPING.USER_ID.eq(targets.userId))
        .orderBy(ConfigTables.SCIM_RESOURCE_MAPPING.ID)
        .fetch()
    val scimOrganizationIds =
      (scimMappingSnapshot.map { requireNotNull(it.value2()) } + targets.organizationIds)
        .distinct()
        .sorted()
    scimOrganizationIds.forEach { organizationId ->
      check(
        ctx
          .select(ConfigTables.ORGANIZATION.ID)
          .from(ConfigTables.ORGANIZATION)
          .where(ConfigTables.ORGANIZATION.ID.eq(organizationId))
          .forUpdate()
          .fetchOne() != null,
      ) {
        "SCIM organization $organizationId changed while deleting DSR user ${targets.userId}; retry the deletion."
      }
    }
    val lockedScimConfigurations =
      if (scimOrganizationIds.isEmpty()) {
        emptyList()
      } else {
        ctx
          .select(ConfigTables.SCIM_CONFIGURATION.ID, ConfigTables.SCIM_CONFIGURATION.ORGANIZATION_ID)
          .from(ConfigTables.SCIM_CONFIGURATION)
          .where(ConfigTables.SCIM_CONFIGURATION.ORGANIZATION_ID.`in`(scimOrganizationIds))
          .orderBy(ConfigTables.SCIM_CONFIGURATION.ID)
          .forUpdate()
          .fetch()
      }
    check(
      scimMappingSnapshot.all { mapping ->
        lockedScimConfigurations.any { configuration ->
          configuration.value1() == mapping.value3() && configuration.value2() == mapping.value2()
        }
      },
    ) {
      "A SCIM configuration changed while deleting DSR user ${targets.userId}; retry the deletion."
    }

    val observedUserEmail =
      ctx
        .select(ConfigTables.USER.EMAIL)
        .from(ConfigTables.USER)
        .where(ConfigTables.USER.ID.eq(targets.userId))
        .fetchOne(ConfigTables.USER.EMAIL)
    (scimMappingSnapshot.mapNotNull { it.value4() } + listOfNotNull(observedUserEmail))
      .distinctBy { it.lowercase(Locale.ROOT) }
      .sortedBy { it.lowercase(Locale.ROOT) }
      .forEach { email ->
        ctx.fetch("SELECT pg_advisory_xact_lock(hashtextextended('email:' || lower(?), 0))", email)
      }
    val lockedUserEmail =
      ctx
        .select(ConfigTables.USER.EMAIL)
        .from(ConfigTables.USER)
        .where(ConfigTables.USER.ID.eq(targets.userId))
        .forUpdate()
        .fetchOne(ConfigTables.USER.EMAIL)
    check(lockedUserEmail == observedUserEmail && lockedUserEmail != null) {
      "Airbyte user ${targets.userId} changed while preparing DSR deletion; retry the deletion."
    }

    val lockedScimMappings =
      ctx
        .select(
          ConfigTables.SCIM_RESOURCE_MAPPING.ID,
          ConfigTables.SCIM_RESOURCE_MAPPING.ORGANIZATION_ID,
          ConfigTables.SCIM_RESOURCE_MAPPING.SCIM_CONFIGURATION_ID,
          ConfigTables.SCIM_RESOURCE_MAPPING.PRIMARY_EMAIL,
        ).from(ConfigTables.SCIM_RESOURCE_MAPPING)
        .where(ConfigTables.SCIM_RESOURCE_MAPPING.RESOURCE_TYPE.eq(ScimResourceType.USER))
        .and(ConfigTables.SCIM_RESOURCE_MAPPING.USER_ID.eq(targets.userId))
        .orderBy(ConfigTables.SCIM_RESOURCE_MAPPING.ID)
        .forUpdate()
        .fetch()
    check(
      lockedScimMappings.size == scimMappingSnapshot.size &&
        lockedScimMappings.zip(scimMappingSnapshot).all { (locked, observed) ->
          locked.value1() == observed.value1() &&
            locked.value2() == observed.value2() &&
            locked.value3() == observed.value3() &&
            locked.value4() == observed.value4()
        },
    ) {
      "SCIM mappings changed while preparing DSR deletion for user ${targets.userId}; retry the deletion."
    }
    ctx
      .deleteFrom(ConfigTables.SCIM_RESOURCE_MAPPING)
      .where(ConfigTables.SCIM_RESOURCE_MAPPING.RESOURCE_TYPE.eq(ScimResourceType.USER))
      .and(ConfigTables.SCIM_RESOURCE_MAPPING.USER_ID.eq(targets.userId))
      .execute()

    val observedCurrentAuthUserIds =
      ctx
        .select(ConfigTables.AUTH_USER.AUTH_USER_ID)
        .from(ConfigTables.AUTH_USER)
        .where(ConfigTables.AUTH_USER.USER_ID.eq(targets.userId))
        .orderBy(ConfigTables.AUTH_USER.AUTH_USER_ID)
        .fetch(ConfigTables.AUTH_USER.AUTH_USER_ID)
        .distinct()
    val authUserIdsToLock =
      (targets.authUserIds + observedCurrentAuthUserIds)
        .distinct()
        .sorted()
    authUserIdsToLock.forEach { authUserId ->
      ctx.fetch("SELECT pg_advisory_xact_lock(hashtextextended('auth-user:' || ?, 0))", authUserId)
      val owners =
        ctx
          .select(ConfigTables.AUTH_USER.USER_ID)
          .from(ConfigTables.AUTH_USER)
          .where(ConfigTables.AUTH_USER.AUTH_USER_ID.eq(authUserId))
          .orderBy(ConfigTables.AUTH_USER.USER_ID)
          .forUpdate()
          .fetch(ConfigTables.AUTH_USER.USER_ID)
          .distinct()
      check(owners == listOf(targets.userId)) {
        "Authentication identity $authUserId is not uniquely owned by DSR user ${targets.userId}."
      }
    }
    val currentAuthUserIds =
      ctx
        .select(ConfigTables.AUTH_USER.AUTH_USER_ID)
        .from(ConfigTables.AUTH_USER)
        .where(ConfigTables.AUTH_USER.USER_ID.eq(targets.userId))
        .orderBy(ConfigTables.AUTH_USER.AUTH_USER_ID)
        .fetch(ConfigTables.AUTH_USER.AUTH_USER_ID)
        .distinct()
    check(currentAuthUserIds == observedCurrentAuthUserIds) {
      "Authentication identities changed while preparing DSR deletion for user ${targets.userId}; retry the deletion."
    }

    val workspaceIds = targets.workspaceIds.distinct()
    val organizationIds = targets.organizationIds.distinct()
    val connectionIds = targets.connectionIds.distinct()
    val actorIdsFromManifest = (targets.sourceIds + targets.destinationIds).distinct()
    val actorIdsFromWorkspaces =
      if (workspaceIds.isEmpty()) {
        emptyList()
      } else {
        ctx
          .select(ConfigTables.ACTOR.ID)
          .from(ConfigTables.ACTOR)
          .where(ConfigTables.ACTOR.WORKSPACE_ID.`in`(workspaceIds))
          .fetch(ConfigTables.ACTOR.ID)
      }
    val actorIds = (actorIdsFromManifest + actorIdsFromWorkspaces).distinct()
    val groupIds = selectUuidByUuidField(ctx, "group", "id", "organization_id", organizationIds)
    val dataplaneGroupIds = selectUuidByUuidField(ctx, "dataplane_group", "id", "organization_id", organizationIds)
    val dataplaneIds = selectUuidByUuidField(ctx, "dataplane", "id", "dataplane_group_id", dataplaneGroupIds)
    val workspaceOrganizationIds = selectUuidByUuidField(ctx, "workspace", "organization_id", "id", workspaceIds)
    val observabilityJobIds =
      selectLongByUuidField(
        ctx = ctx,
        tableName = "observability_jobs_stats",
        selectFieldName = "job_id",
        whereFieldName = "connection_id",
        ids = connectionIds,
      )

    deleteByLongField(ctx, "observability_stream_stats", "job_id", observabilityJobIds)
    deleteByLongField(ctx, "observability_jobs_stats", "job_id", observabilityJobIds)
    deleteDataWorkerUsageRows(ctx, "data_worker_usage_reservation", workspaceIds, organizationIds, workspaceOrganizationIds)
    deleteDataWorkerUsageRows(ctx, "data_worker_usage", workspaceIds, organizationIds, workspaceOrganizationIds)

    deleteByUuidField(ctx, "data_worker_allocated_capacity", "organization_id", organizationIds)
    deleteByUuidField(ctx, "connection_operation", "connection_id", connectionIds)
    deleteByUuidField(ctx, "connection_tag", "connection_id", connectionIds)
    deleteByUuidField(ctx, "connection_timeline_event", "connection_id", connectionIds)
    deleteByUuidField(ctx, "notification_configuration", "connection_id", connectionIds)
    deleteByUuidField(ctx, "schema_management", "connection_id", connectionIds)
    deleteByUuidField(ctx, "state", "connection_id", connectionIds)
    deleteByUuidField(ctx, "stream_generation", "connection_id", connectionIds)
    deleteByUuidField(ctx, "stream_refreshes", "connection_id", connectionIds)
    deleteByUuidField(ctx, "stream_reset", "connection_id", connectionIds)

    val connectionsDeleted =
      if (connectionIds.isEmpty()) {
        0
      } else {
        ctx
          .deleteFrom(ConfigTables.CONNECTION)
          .where(ConfigTables.CONNECTION.ID.`in`(connectionIds))
          .execute()
      }

    deleteByUuidField(ctx, "actor_catalog_fetch_event", "actor_id", actorIds)

    val builderProjectConditions =
      listOfNotNull(
        targets.connectorBuilderProjectIds.distinct().takeIf { it.isNotEmpty() }?.let {
          ConfigTables.CONNECTOR_BUILDER_PROJECT.ID.`in`(it)
        },
        workspaceIds.takeIf { it.isNotEmpty() }?.let {
          ConfigTables.CONNECTOR_BUILDER_PROJECT.WORKSPACE_ID.`in`(it)
        },
      )
    val builderProjectsDeleted =
      if (builderProjectConditions.isEmpty()) {
        0
      } else {
        ctx
          .deleteFrom(ConfigTables.CONNECTOR_BUILDER_PROJECT)
          .where(orCondition(builderProjectConditions))
          .execute()
      }

    deleteByUuidField(ctx, "actor_oauth_parameter", "workspace_id", workspaceIds)
    deleteByUuidField(ctx, "actor_oauth_parameter", "organization_id", organizationIds)

    val actorsDeleted =
      if (actorIds.isEmpty()) {
        0
      } else {
        ctx
          .deleteFrom(ConfigTables.ACTOR)
          .where(ConfigTables.ACTOR.ID.`in`(actorIds))
          .execute()
      }

    deleteByUuidField(ctx, "actor_definition_workspace_grant", "workspace_id", workspaceIds)
    deleteByUuidField(ctx, "actor_definition_workspace_grant", "scope_id", workspaceIds + organizationIds)
    deleteByUuidField(ctx, "secret_persistence_config", "scope_id", workspaceIds + organizationIds)

    deleteByUuidField(ctx, "user_invitation", "accepted_by_user_id", listOf(targets.userId))
    deleteByUuidField(ctx, "user_invitation", "scope_id", workspaceIds + organizationIds)

    deleteByUuidField(ctx, "group_member", "user_id", listOf(targets.userId))
    deleteByUuidField(ctx, "group_member", "group_id", groupIds)

    val permissionConditions = mutableListOf<Condition>(ConfigTables.PERMISSION.USER_ID.eq(targets.userId))
    if (workspaceIds.isNotEmpty()) {
      permissionConditions.add(ConfigTables.PERMISSION.WORKSPACE_ID.`in`(workspaceIds))
    }
    if (organizationIds.isNotEmpty()) {
      permissionConditions.add(ConfigTables.PERMISSION.ORGANIZATION_ID.`in`(organizationIds))
    }
    if (groupIds.isNotEmpty()) {
      permissionConditions.add(ConfigTables.PERMISSION.GROUP_ID.`in`(groupIds))
    }
    val permissionsDeleted =
      ctx
        .deleteFrom(ConfigTables.PERMISSION)
        .where(orCondition(permissionConditions))
        .execute()

    deleteByUuidField(ctx, "group", "id", groupIds)

    deleteByUuidField(ctx, "tag", "workspace_id", workspaceIds)
    deleteByUuidField(ctx, "operation", "workspace_id", workspaceIds)
    deleteByUuidField(ctx, "private_link", "workspace_id", workspaceIds)
    deleteByUuidField(ctx, "private_link", "dataplane_group_id", dataplaneGroupIds)
    deleteWhereAnyUuidMatches(
      ctx,
      "scoped_configuration",
      listOf(
        "resource_id" to connectionIds + actorIds + workspaceIds + organizationIds,
        "scope_id" to workspaceIds + organizationIds,
      ),
    )

    deleteByUuidField(ctx, "dataplane_client_credentials", "dataplane_id", dataplaneIds)
    deleteByUuidField(ctx, "dataplane_heartbeat_log", "dataplane_id", dataplaneIds)
    deleteByUuidField(ctx, "dataplane", "dataplane_group_id", dataplaneGroupIds)
    deleteByUuidField(ctx, "dataplane_network_config", "dataplane_group_id", dataplaneGroupIds)
    deleteByUuidField(ctx, "dataplane_group", "organization_id", organizationIds)

    deleteByUuidField(ctx, "organization_domain_verification", "organization_id", organizationIds)
    deleteByUuidField(ctx, "organization_domain_verification", "created_by", listOf(targets.userId))
    deleteByUuidField(ctx, "organization_email_domain", "organization_id", organizationIds)
    deleteByUuidField(ctx, "organization_payment_config", "organization_id", organizationIds)
    deleteByUuidField(ctx, "billing_notification_settings", "organization_id", organizationIds)
    deleteByUuidField(ctx, "sso_config", "organization_id", organizationIds)

    deleteByStringField(ctx, "application", "auth_user_id", currentAuthUserIds)

    val authUsersDeleted =
      ctx
        .deleteFrom(ConfigTables.AUTH_USER)
        .where(ConfigTables.AUTH_USER.USER_ID.eq(targets.userId))
        .execute()

    val userUpdated =
      ctx
        .update(ConfigTables.USER)
        .set(ConfigTables.USER.NAME, datagrailId)
        .set(ConfigTables.USER.EMAIL, datagrailId)
        .set(ConfigTables.USER.DEFAULT_WORKSPACE_ID, null as UUID?)
        .where(ConfigTables.USER.ID.eq(targets.userId))
        .execute()
    if (userUpdated != 1) {
      throw IllegalStateException(
        "Expected to tombstone exactly one Airbyte user row for DSR userId=${targets.userId}, but updated $userUpdated.",
      )
    }

    val workspacesDeleted =
      if (workspaceIds.isEmpty()) {
        0
      } else {
        ctx
          .deleteFrom(ConfigTables.WORKSPACE)
          .where(ConfigTables.WORKSPACE.ID.`in`(workspaceIds))
          .execute()
      }

    val organizationsDeleted =
      if (organizationIds.isEmpty()) {
        0
      } else {
        ctx
          .deleteFrom(ConfigTables.ORGANIZATION)
          .where(ConfigTables.ORGANIZATION.ID.`in`(organizationIds))
          .execute()
      }

    return ConfigDeletionCounts(
      connections = connectionsDeleted,
      actors = actorsDeleted,
      builderProjects = builderProjectsDeleted,
      permissions = permissionsDeleted,
      workspaces = workspacesDeleted,
      organizations = organizationsDeleted,
      authUsers = authUsersDeleted,
      userTombstoned = true,
    )
  }

  private fun namedTable(tableName: String): Table<*> = DSL.table(DSL.name("public", tableName))

  private fun uuidField(
    tableName: String,
    fieldName: String,
  ): Field<UUID> = DSL.field(DSL.name("public", tableName, fieldName), UUID::class.java)

  private fun stringField(
    tableName: String,
    fieldName: String,
  ): Field<String> = DSL.field(DSL.name("public", tableName, fieldName), String::class.java)

  private fun longField(
    tableName: String,
    fieldName: String,
  ): Field<Long> = DSL.field(DSL.name("public", tableName, fieldName), Long::class.java)

  private fun selectUuidByUuidField(
    ctx: DSLContext,
    tableName: String,
    selectFieldName: String,
    whereFieldName: String,
    ids: List<UUID>,
  ): List<UUID> {
    if (ids.isEmpty()) {
      return emptyList()
    }
    val selectField = uuidField(tableName, selectFieldName)
    return ctx
      .select(selectField)
      .from(namedTable(tableName))
      .where(uuidField(tableName, whereFieldName).`in`(ids))
      .fetch(selectField)
      .filterNotNull()
      .distinct()
  }

  private fun selectLongByUuidField(
    ctx: DSLContext,
    tableName: String,
    selectFieldName: String,
    whereFieldName: String,
    ids: List<UUID>,
  ): List<Long> {
    if (ids.isEmpty()) {
      return emptyList()
    }
    val selectField = longField(tableName, selectFieldName)
    return ctx
      .select(selectField)
      .from(namedTable(tableName))
      .where(uuidField(tableName, whereFieldName).`in`(ids.distinct()))
      .fetch(selectField)
      .filterNotNull()
      .distinct()
  }

  private fun deleteDataWorkerUsageRows(
    ctx: DSLContext,
    tableName: String,
    workspaceIds: List<UUID>,
    organizationIds: List<UUID>,
    workspaceOrganizationIds: List<UUID>,
  ): Int {
    val deletedByOrganization = deleteByUuidField(ctx, tableName, "organization_id", organizationIds)
    val deletedOrganizationIds = organizationIds.toSet()
    val workspaceScopedOrganizationIds = workspaceOrganizationIds.filterNot { deletedOrganizationIds.contains(it) }.distinct()
    if (workspaceIds.isEmpty() || workspaceScopedOrganizationIds.isEmpty()) {
      return deletedByOrganization
    }

    val deletedByWorkspace =
      ctx
        .deleteFrom(namedTable(tableName))
        .where(uuidField(tableName, "organization_id").`in`(workspaceScopedOrganizationIds))
        .and(uuidField(tableName, "workspace_id").`in`(workspaceIds.distinct()))
        .execute()
    return deletedByOrganization + deletedByWorkspace
  }

  private fun deleteByUuidField(
    ctx: DSLContext,
    tableName: String,
    fieldName: String,
    ids: List<UUID>,
  ): Int {
    if (ids.isEmpty()) {
      return 0
    }
    return ctx
      .deleteFrom(namedTable(tableName))
      .where(uuidField(tableName, fieldName).`in`(ids))
      .execute()
  }

  private fun deleteByStringField(
    ctx: DSLContext,
    tableName: String,
    fieldName: String,
    ids: List<String>,
  ): Int {
    if (ids.isEmpty()) {
      return 0
    }
    return ctx
      .deleteFrom(namedTable(tableName))
      .where(stringField(tableName, fieldName).`in`(ids))
      .execute()
  }

  private fun deleteByLongField(
    ctx: DSLContext,
    tableName: String,
    fieldName: String,
    ids: List<Long>,
  ): Int {
    if (ids.isEmpty()) {
      return 0
    }
    return ctx
      .deleteFrom(namedTable(tableName))
      .where(longField(tableName, fieldName).`in`(ids))
      .execute()
  }

  private fun deleteWhereAnyUuidMatches(
    ctx: DSLContext,
    tableName: String,
    filters: List<Pair<String, List<UUID>>>,
  ): Int {
    val condition = anyUuidMatchCondition(tableName, filters) ?: return 0
    return ctx.deleteFrom(namedTable(tableName)).where(condition).execute()
  }

  private fun anyUuidMatchCondition(
    tableName: String,
    filters: List<Pair<String, List<UUID>>>,
  ): Condition? {
    val conditions =
      filters
        .filter { (_, ids) -> ids.isNotEmpty() }
        .map { (fieldName, ids) -> uuidField(tableName, fieldName).`in`(ids.distinct()) }
    return if (conditions.isEmpty()) null else orCondition(conditions)
  }

  private fun orCondition(conditions: List<Condition>): Condition = conditions.reduce { left, right -> left.or(right) }

  data class ConfigDeletionCounts(
    val connections: Int,
    val actors: Int,
    val builderProjects: Int,
    val permissions: Int,
    val workspaces: Int,
    val organizations: Int,
    val authUsers: Int,
    val userTombstoned: Boolean,
  ) {
    companion object {
      fun empty() =
        ConfigDeletionCounts(
          connections = 0,
          actors = 0,
          builderProjects = 0,
          permissions = 0,
          workspaces = 0,
          organizations = 0,
          authUsers = 0,
          userTombstoned = false,
        )
    }
  }
}
