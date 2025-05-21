/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.sql.Date
import java.time.LocalDate
import java.util.UUID
import kotlin.jvm.optionals.getOrNull
import kotlin.test.assertNotNull
import kotlin.test.assertNull

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_50_41_012__BreakingChangePinDataMigrationTest : AbstractConfigsDatabaseTest() {
  private var migration: V0_50_41_012__BreakingChangePinDataMigration? = null

  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_50_41_012__BreakingChangePinDataMigrationTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V0_50_41_009__AddBreakingChangeConfigOrigin()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()

    migration = V0_50_41_012__BreakingChangePinDataMigration()
  }

  @ParameterizedTest
  @MethodSource("testMethodSource")
  fun testBreakingChangeOriginScopedConfig(
    actorVersion: String,
    existingConfigScopes: List<V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType>,
    expectedBCOrigin: String?,
  ) {
    val ctx = getDslContext()

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;")

    val actorDefinitionId = UUID.randomUUID()
    createActorDefinition(ctx, actorDefinitionId)

    val defaultVersionId = UUID.randomUUID()
    val defaultVersionTag = "3.1.0"
    createActorDefinitionVersion(ctx, defaultVersionId, actorDefinitionId, defaultVersionTag)
    setActorDefinitionDefaultVersion(ctx, actorDefinitionId, defaultVersionId)

    var actorVersionId = defaultVersionId
    if (actorVersion != defaultVersionTag) {
      actorVersionId = UUID.randomUUID()
      createActorDefinitionVersion(ctx, actorVersionId, actorDefinitionId, actorVersion)
    }

    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    createWorkspace(ctx, workspaceId, organizationId)

    val actorId = UUID.randomUUID()
    createActor(ctx, actorId, workspaceId, actorDefinitionId, actorVersionId)

    for (existingConfigScope in existingConfigScopes) {
      val scopeId =
        when (existingConfigScope) {
          V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType.ACTOR -> actorId
          V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType.WORKSPACE -> workspaceId
          V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType.ORGANIZATION -> organizationId
          else -> throw IllegalArgumentException("Unexpected config scope type: $existingConfigScope")
        }
      createScopedConfig(
        ctx,
        actorDefinitionId,
        existingConfigScope,
        scopeId,
        V0_50_41_009__AddBreakingChangeConfigOrigin.ConfigOriginType.USER,
        "userId",
        actorVersion,
      )
    }

    val breakingChanges = listOf("1.0.0", "2.0.0", "3.0.0")
    for (breakingChange in breakingChanges) {
      createBreakingChange(ctx, actorDefinitionId, breakingChange)
    }

    // run migration
    migration!!.migrateBreakingChangePins(ctx)

    // get pin and assert it's correct
    val scopedConfig = getScopedConfig(ctx, actorDefinitionId, actorId)
    if (expectedBCOrigin == null) {
      assertNull(scopedConfig)
    } else {
      assertNotNull(scopedConfig)
      assert(scopedConfig["value"] == actorVersionId.toString())
      assert(scopedConfig["origin"] == expectedBCOrigin)
    }
  }

  companion object {
    @JvmStatic
    fun testMethodSource(): List<Arguments> =
      listOf( // Already on latest (3.1.0), no BC pin
        Arguments.of(
          "3.1.0",
          listOf<Any>(),
          null,
        ), // Held back on an older version should create pin with correct BC as origin
        Arguments.of("0.1.0", listOf<Any>(), "1.0.0"),
        Arguments.of(
          "1.0.0",
          listOf<Any>(),
          "2.0.0",
        ), // Actors already pinned (at any level) should be ignored
        Arguments.of(
          "1.0.0",
          listOf(
            V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType.ACTOR,
          ),
          null,
        ),
        Arguments.of(
          "1.0.0",
          listOf(
            V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType.WORKSPACE,
          ),
          null,
        ),
        Arguments.of(
          "1.0.0",
          listOf(
            V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType.ORGANIZATION,
          ),
          null,
        ),
        Arguments.of(
          "1.0.0",
          listOf(
            V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType.ACTOR,
            V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType.WORKSPACE,
            V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType.ORGANIZATION,
          ),
          null,
        ),
      )

    private fun createActorDefinition(
      ctx: DSLContext,
      actorDefinitionId: UUID,
    ) {
      ctx
        .insertInto(DSL.table("actor_definition"))
        .columns(
          DSL.field("id"),
          DSL.field("name"),
          DSL.field("actor_type"),
        ).values(
          actorDefinitionId,
          "postgres",
          V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.source,
        ).execute()
    }

    private fun setActorDefinitionDefaultVersion(
      ctx: DSLContext,
      actorDefinitionId: UUID,
      defaultVersionId: UUID,
    ) {
      ctx
        .update(DSL.table("actor_definition"))
        .set(DSL.field("default_version_id"), defaultVersionId)
        .where(DSL.field("id").eq(actorDefinitionId))
        .execute()
    }

    private fun createActorDefinitionVersion(
      ctx: DSLContext,
      actorDefinitionVersionId: UUID,
      actorDefinitionId: UUID,
      version: String,
    ) {
      ctx
        .insertInto(DSL.table("actor_definition_version"))
        .columns(
          DSL.field("id"),
          DSL.field("actor_definition_id"),
          DSL.field("docker_repository"),
          DSL.field("docker_image_tag"),
          DSL.field("support_level"),
          DSL.field("spec", SQLDataType.JSONB),
        ).values(
          actorDefinitionVersionId,
          actorDefinitionId,
          "airbyte/postgres",
          version,
          V0_50_41_006__AlterSupportLevelAddArchived.SupportLevel.community,
          JSONB.valueOf("{}"),
        ).execute()
    }

    private fun createActor(
      ctx: DSLContext,
      actorId: UUID,
      workspaceId: UUID,
      actorDefinitionId: UUID,
      defaultVersionId: UUID,
    ) {
      ctx
        .insertInto(DSL.table("actor"))
        .columns(
          DSL.field("id"),
          DSL.field("name"),
          DSL.field("actor_type"),
          DSL.field("workspace_id"),
          DSL.field("actor_definition_id"),
          DSL.field("default_version_id"),
          DSL.field("configuration", SQLDataType.JSONB),
        ).values(
          actorId,
          "postgres",
          V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.source,
          workspaceId,
          actorDefinitionId,
          defaultVersionId,
          JSONB.valueOf("{}"),
        ).execute()
    }

    private fun createWorkspace(
      ctx: DSLContext,
      workspaceId: UUID,
      organizationId: UUID,
    ) {
      ctx
        .insertInto(DSL.table("workspace"))
        .columns(
          DSL.field("id"),
          DSL.field("name"),
          DSL.field("slug"),
          DSL.field("initial_setup_complete"),
          DSL.field("organization_id"),
        ).values(
          workspaceId,
          "workspace",
          "workspace",
          true,
          organizationId,
        ).execute()
    }

    private fun createScopedConfig(
      ctx: DSLContext,
      actorDefinitionId: UUID,
      scopeType: V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType,
      scopeId: UUID,
      originType: V0_50_41_009__AddBreakingChangeConfigOrigin.ConfigOriginType,
      origin: String,
      value: String,
    ) {
      ctx
        .insertInto(DSL.table("scoped_configuration"))
        .columns(
          DSL.field("id"),
          DSL.field("key"),
          DSL.field("resource_type"),
          DSL.field("resource_id"),
          DSL.field("scope_type"),
          DSL.field("scope_id"),
          DSL.field("value"),
          DSL.field("origin_type"),
          DSL.field("origin"),
        ).values(
          UUID.randomUUID(),
          "connector_version",
          V0_50_33_014__AddScopedConfigurationTable.ConfigResourceType.ACTOR_DEFINITION,
          actorDefinitionId,
          scopeType,
          scopeId,
          value,
          originType,
          origin,
        ).execute()
    }

    private fun getScopedConfig(
      ctx: DSLContext,
      actorDefinitionId: UUID,
      scopeId: UUID,
    ): Map<String, String>? =
      ctx
        .select(DSL.field("value"), DSL.field("origin"))
        .from(DSL.table("scoped_configuration"))
        .where(
          DSL
            .field("resource_type")
            .eq(V0_50_33_014__AddScopedConfigurationTable.ConfigResourceType.ACTOR_DEFINITION)
            .and(DSL.field("resource_id").eq(actorDefinitionId))
            .and(DSL.field("scope_type").eq(V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType.ACTOR))
            .and(DSL.field("scope_id").eq(scopeId))
            .and(DSL.field("origin_type").eq(V0_50_41_009__AddBreakingChangeConfigOrigin.ConfigOriginType.BREAKING_CHANGE)),
        ).fetchOptional()
        .getOrNull()
        ?.let { record ->
          mapOf(
            "value" to record.get(DSL.field("value", String::class.java)),
            "origin" to record.get(DSL.field("origin", String::class.java)),
          )
        }

    private fun createBreakingChange(
      ctx: DSLContext,
      actorDefinitionId: UUID,
      version: String,
    ) {
      ctx
        .insertInto(DSL.table("actor_definition_breaking_change"))
        .columns(
          DSL.field("actor_definition_id"),
          DSL.field("version"),
          DSL.field("migration_documentation_url"),
          DSL.field("message"),
          DSL.field("upgrade_deadline", SQLDataType.DATE),
        ).values(
          actorDefinitionId,
          version,
          "https://docs.airbyte.io/",
          "Breaking change",
          Date.valueOf(LocalDate.now()),
        ).execute()
    }
  }
}
