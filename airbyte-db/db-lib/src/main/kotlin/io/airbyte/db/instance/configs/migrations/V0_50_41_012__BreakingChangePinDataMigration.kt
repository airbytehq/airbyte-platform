/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.version.Version
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.annotation.Nullable
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.Record1
import org.jooq.Record2
import org.jooq.Record3
import org.jooq.Record4
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.util.UUID

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V0_50_41_012__BreakingChangePinDataMigration : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    migrateBreakingChangePins(ctx)
  }

  @VisibleForTesting
  fun migrateBreakingChangePins(ctx: DSLContext) {
    getActorDefinitions(ctx).forEach {
      migrateBreakingChangePinsForDefinition(ctx, it)
    }
  }

  private fun migrateBreakingChangePinsForDefinition(
    ctx: DSLContext,
    actorDefinition: ActorDefinition,
  ) {
    val unpinnedActorsNotOnDefaultVersion = getUnpinnedActorsNotOnDefaultVersion(ctx, actorDefinition)
    val breakingChangeVersions = getBreakingChangeVersionsForDefinition(ctx, actorDefinition.actorDefinitionId)
    unpinnedActorsNotOnDefaultVersion.forEach { actor ->
      val originatingBreakingChange =
        getOriginatingBreakingChangeForVersion(
          ctx,
          actor.defaultVersionId,
          breakingChangeVersions,
        )
      createScopedConfiguration(
        ctx,
        actorDefinition.actorDefinitionId,
        originatingBreakingChange,
        V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType.ACTOR,
        actor.actorId,
        actor.defaultVersionId,
      )
    }
  }

  private fun getUnpinnedActorsNotOnDefaultVersion(
    ctx: DSLContext,
    actorDefinition: ActorDefinition,
  ): List<Actor> {
    val actors = getActorsNotOnDefaultVersion(ctx, actorDefinition)
    val actorIdsWithConfig =
      getIdsWithConfig(
        ctx,
        actorDefinition.actorDefinitionId,
        V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType.ACTOR,
        actors.map { it.actorId }.toList(),
      )
    val workspaceIdsWithConfig =
      getIdsWithConfig(
        ctx,
        actorDefinition.actorDefinitionId,
        V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType.WORKSPACE,
        actors.map { it.workspaceId }.toList(),
      )
    val orgIdsWithConfig =
      getIdsWithConfig(
        ctx,
        actorDefinition.actorDefinitionId,
        V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType.ORGANIZATION,
        actors.map { it.organizationId }.toList(),
      )

    return actors
      .filter { actor: Actor -> !actorIdsWithConfig.contains(actor.actorId) }
      .filter { actor: Actor -> !workspaceIdsWithConfig.contains(actor.workspaceId) }
      .filter { actor: Actor -> !orgIdsWithConfig.contains(actor.organizationId) }
      .toList()
  }

  val versionBreakingChangeCache = mutableMapOf<UUID, String>()

  private fun getOriginatingBreakingChangeForVersion(
    ctx: DSLContext,
    versionId: UUID,
    breakingChangeVersions: List<Version>,
  ): String? {
    if (versionBreakingChangeCache.containsKey(versionId)) {
      return versionBreakingChangeCache[versionId]
    }

    val version = getActorDefinitionVersion(ctx, versionId)
    val pinnedVersion = Version(version!!.dockerImageTag)

    val breakingVersion =
      breakingChangeVersions
        .firstOrNull { breakingChangeVersion: Version -> breakingChangeVersion.greaterThan(pinnedVersion) }

    check(breakingVersion != null) {
      "Could not find a corresponding breaking change for pinned version ${version.dockerImageTag} on " +
        "actor definition ID ${version.actorDefinitionId}. Overriding actor versions without a breaking change is not supported."
    }

    val originatingBreakingChange = breakingVersion.serialize()
    versionBreakingChangeCache[versionId] = originatingBreakingChange
    return originatingBreakingChange
  }

  private fun getIdsWithConfig(
    ctx: DSLContext,
    actorDefinitionId: UUID,
    scopeType: V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType,
    scopeIds: List<UUID?>,
  ): List<UUID> =
    ctx
      .select(SCOPE_ID)
      .from(SCOPED_CONFIGURATION)
      .where(KEY.eq(CONNECTOR_VERSION_KEY))
      .and(RESOURCE_TYPE.eq(V0_50_33_014__AddScopedConfigurationTable.ConfigResourceType.ACTOR_DEFINITION))
      .and(RESOURCE_ID.eq(actorDefinitionId))
      .and(SCOPE_TYPE.eq(scopeType))
      .and(SCOPE_ID.`in`(scopeIds))
      .fetch()
      .map { r: Record1<UUID> ->
        r.get(
          SCOPE_ID,
        )
      }

  private fun createScopedConfiguration(
    ctx: DSLContext,
    actorDefinitionId: UUID,
    breakingChangeVersionTag: String?,
    scopeType: V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType,
    scopeId: UUID,
    pinnedVersionId: UUID,
  ) {
    ctx
      .insertInto(SCOPED_CONFIGURATION)
      .columns(ID, KEY, RESOURCE_TYPE, RESOURCE_ID, SCOPE_TYPE, SCOPE_ID, ORIGIN_TYPE, ORIGIN, VALUE, DESCRIPTION)
      .values(
        UUID.randomUUID(),
        CONNECTOR_VERSION_KEY,
        V0_50_33_014__AddScopedConfigurationTable.ConfigResourceType.ACTOR_DEFINITION,
        actorDefinitionId,
        scopeType,
        scopeId,
        V0_50_41_009__AddBreakingChangeConfigOrigin.ConfigOriginType.BREAKING_CHANGE,
        breakingChangeVersionTag,
        pinnedVersionId.toString(),
        "Automated breaking change pin migration",
      ).execute()
  }

  private fun getActorDefinitions(ctx: DSLContext): List<ActorDefinition> {
    val id = DSL.field("id", SQLDataType.UUID)
    val defaultVersionId = DSL.field("default_version_id", SQLDataType.UUID)

    return ctx
      .select(id, defaultVersionId)
      .from(ACTOR_DEFINITION)
      .fetch()
      .map { r: Record2<UUID, UUID> ->
        ActorDefinition(
          r.get(id),
          r.get(defaultVersionId),
        )
      }
  }

  private fun getActorDefinitionVersion(
    ctx: DSLContext,
    versionId: UUID,
  ): ActorDefinitionVersion? {
    val id = DSL.field("id", SQLDataType.UUID)
    val actorDefinitionId = DSL.field("actor_definition_id", SQLDataType.UUID)
    val dockerImageTag = DSL.field("docker_image_tag", SQLDataType.VARCHAR)

    return ctx
      .select(id, actorDefinitionId, dockerImageTag)
      .from("actor_definition_version")
      .where(id.eq(versionId))
      .fetchOne { r: Record3<UUID, UUID, String> ->
        ActorDefinitionVersion(
          r.get(id),
          r.get(actorDefinitionId),
          r.get(dockerImageTag),
        )
      }
  }

  private fun getBreakingChangeVersionsForDefinition(
    ctx: DSLContext,
    actorDefinitionId: UUID,
  ): List<Version> {
    val actorDefinitionIdField = DSL.field("actor_definition_id", SQLDataType.UUID)
    val version = DSL.field("version", SQLDataType.VARCHAR)

    return ctx
      .select(version)
      .from(ACTOR_DEFINITION_BREAKING_CHANGE)
      .where(actorDefinitionIdField.eq(actorDefinitionId))
      .fetch()
      .map { r: Record1<String> -> Version(r.get(version)) }
      .sortedWith { a, b -> a.versionCompareTo(b) }
  }

  private fun getActorsNotOnDefaultVersion(
    ctx: DSLContext,
    actorDefinition: ActorDefinition,
  ): List<Actor> {
    // Actor fields
    val actorId = DSL.field("actor.id", SQLDataType.UUID)
    val actorWorkspaceId = DSL.field("actor.workspace_id", SQLDataType.UUID)
    val actorDefaultVersionId = DSL.field("actor.default_version_id", SQLDataType.UUID)
    val actorDefinitionId = DSL.field("actor.actor_definition_id", SQLDataType.UUID)

    // Workspace fields
    val workspaceId = DSL.field("workspace.id", SQLDataType.UUID)
    val workspaceOrgId = DSL.field("workspace.organization_id", SQLDataType.UUID)

    return ctx
      .select(actorId, actorWorkspaceId, workspaceOrgId, actorDefaultVersionId)
      .from(ACTOR)
      .join(WORKSPACE)
      .on(workspaceId.eq(actorWorkspaceId))
      .where(actorDefinitionId.eq(actorDefinition.actorDefinitionId))
      .and(actorDefaultVersionId.ne(actorDefinition.defaultVersionId))
      .fetch()
      .map { r: Record4<UUID, UUID, UUID, UUID> ->
        Actor(
          r.get(actorId),
          r.get(actorWorkspaceId),
          r.get(workspaceOrgId),
          r.get(actorDefaultVersionId),
        )
      }
  }

  @JvmRecord
  internal data class ActorDefinition(
    val actorDefinitionId: UUID,
    val defaultVersionId: UUID?,
  )

  @JvmRecord
  internal data class ActorDefinitionVersion(
    val versionId: UUID,
    val actorDefinitionId: UUID,
    val dockerImageTag: String,
  )

  @JvmRecord
  internal data class Actor(
    val actorId: UUID,
    val workspaceId: UUID,
    @field:Nullable @param:Nullable val organizationId: UUID,
    val defaultVersionId: UUID,
  )

  companion object {
    private const val CONNECTOR_VERSION_KEY = "connector_version"
    private val ACTOR = DSL.table("actor")
    private val ACTOR_DEFINITION = DSL.table("actor_definition")
    private val ACTOR_DEFINITION_BREAKING_CHANGE = DSL.table("actor_definition_breaking_change")
    private val WORKSPACE = DSL.table("workspace")
    private val SCOPED_CONFIGURATION = DSL.table("scoped_configuration")
    private val ID = DSL.field("id", SQLDataType.UUID)
    private val KEY = DSL.field("key", SQLDataType.VARCHAR)
    private val RESOURCE_TYPE = DSL.field("resource_type", V0_50_33_014__AddScopedConfigurationTable.ConfigResourceType::class.java)
    private val RESOURCE_ID = DSL.field("resource_id", SQLDataType.UUID)
    private val SCOPE_TYPE = DSL.field("scope_type", V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType::class.java)
    private val SCOPE_ID = DSL.field("scope_id", SQLDataType.UUID)
    private val VALUE = DSL.field("value", SQLDataType.VARCHAR)
    private val DESCRIPTION = DSL.field("description", SQLDataType.VARCHAR)
    private val ORIGIN_TYPE = DSL.field("origin_type", V0_50_41_009__AddBreakingChangeConfigOrigin.ConfigOriginType::class.java)
    private val ORIGIN = DSL.field("origin", SQLDataType.VARCHAR)
  }
}
