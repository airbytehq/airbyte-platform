/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.migrations.V0_50_19_001__CreateDefaultOrganizationAndUser.Companion.createDefaultUserAndOrganization
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.exception.DataAccessException
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_50_5_005__AddScopeToActorDefinitionWorkspaceGrantTableTest : AbstractConfigsDatabaseTest() {
  private var devConfigsDbMigrator: DevDatabaseMigrator? = null

  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_50_5_005__AddScopeToActorDefinitionWorkspaceGrantTableTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)

    val previousMigration: BaseJavaMigration = V0_50_5_004__AddActorDefinitionBreakingChangeTable()
    devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator!!.createBaseline()
  }

  @AfterEach
  fun afterEach() {
    // Making sure we reset between tests
    dslContext!!.dropSchemaIfExists("public").cascade().execute()
    dslContext!!.createSchema("public").execute()
    dslContext!!.setSchema("public").execute()
  }

  @Test
  fun testSimpleMigration() {
    val context = dslContext!!
    val actorDefinitionId = UUID.randomUUID()
    val workspaceId1 = UUID.randomUUID()
    val workspaceId2 = UUID.randomUUID()

    addDefaultOrganization(context)

    addWorkspace(context, workspaceId1)
    addWorkspace(context, workspaceId2)

    addActorDefinition(context, actorDefinitionId)

    // Adding initial actor_definition_workspace_grant
    context
      .insertInto(DSL.table(ACTOR_DEFINITION_WORKSPACE_GRANT_TABLE))
      .columns(
        DSL.field(ACTOR_DEFINITION_ID),
        DSL.field(WORKSPACE_ID),
      ).values(
        actorDefinitionId,
        workspaceId1,
      ).execute()

    context
      .insertInto(DSL.table(ACTOR_DEFINITION_WORKSPACE_GRANT_TABLE))
      .columns(
        DSL.field(ACTOR_DEFINITION_ID),
        DSL.field(WORKSPACE_ID),
      ).values(
        actorDefinitionId,
        workspaceId2,
      ).execute()

    // Applying the migration
    devConfigsDbMigrator!!.migrate()

    Assertions.assertTrue(scopeColumnsExists(context), "column is missing")
    Assertions.assertTrue(
      scopeColumnsMatchWorkspaceId(context, actorDefinitionId, workspaceId1),
      "workspace id 1 doesn't match",
    )
    Assertions.assertTrue(
      scopeColumnsMatchWorkspaceId(context, actorDefinitionId, workspaceId2),
      "workspace id 2 doesn't match",
    )
  }

  @Test
  fun testUniquenessConstraint() {
    devConfigsDbMigrator!!.migrate()
    val actorDefinitionId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val scopeId = UUID.randomUUID()

    val context = dslContext!!

    // We retroactively made orgs required so applying the default org/user migration so we can use the
    // default org to allow this test to pass
    createDefaultUserAndOrganization(context)
    context
      .alterTable("workspace")
      .alterColumn("dataplane_group_id")
      .dropNotNull()
      .execute()

    addWorkspace(context, workspaceId)
    addActorDefinition(context, actorDefinitionId)

    context
      .insertInto(DSL.table(ACTOR_DEFINITION_WORKSPACE_GRANT_TABLE))
      .columns(
        DSL.field(ACTOR_DEFINITION_ID),
        DSL.field(SCOPE_ID),
        DSL.field(SCOPE_TYPE),
      ).values(
        actorDefinitionId,
        scopeId,
        V0_50_5_005__AddScopeToActorDefinitionWorkspaceGrantTable.ScopeTypeEnum.workspace,
      ).execute()

    context
      .insertInto(DSL.table(ACTOR_DEFINITION_WORKSPACE_GRANT_TABLE))
      .columns(
        DSL.field(ACTOR_DEFINITION_ID),
        DSL.field(SCOPE_ID),
        DSL.field(SCOPE_TYPE),
      ).values(
        actorDefinitionId,
        scopeId,
        V0_50_5_005__AddScopeToActorDefinitionWorkspaceGrantTable.ScopeTypeEnum.organization,
      ).execute()

    Assertions.assertThrows(
      DataAccessException::class.java,
    ) {
      context
        .insertInto(
          DSL.table(
            ACTOR_DEFINITION_WORKSPACE_GRANT_TABLE,
          ),
        ).columns(
          DSL.field(ACTOR_DEFINITION_ID),
          DSL.field(SCOPE_ID),
          DSL.field(SCOPE_TYPE),
        ).values(
          actorDefinitionId,
          scopeId,
          V0_50_5_005__AddScopeToActorDefinitionWorkspaceGrantTable.ScopeTypeEnum.workspace,
        ).execute()
    }
  }

  companion object {
    private const val ACTOR_DEFINITION_WORKSPACE_GRANT_TABLE = "actor_definition_workspace_grant"
    private const val ACTOR_DEFINITION_ID = "actor_definition_id"
    private const val WORKSPACE_ID = "workspace_id"
    private const val SCOPE_ID = "scope_id"
    private const val SCOPE_TYPE = "scope_type"
    val DEFAULT_UUID: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")

    private fun addDefaultOrganization(context: DSLContext) {
      val idColumn = DSL.field("id", SQLDataType.UUID)
      val emailColumn = DSL.field("email", SQLDataType.VARCHAR(256))
      val nameColumn = DSL.field("name", SQLDataType.VARCHAR(256))
      val userIdColumn = DSL.field("user_id", SQLDataType.UUID)

      context
        .insertInto(DSL.table("organization"))
        .columns(idColumn, emailColumn, nameColumn, userIdColumn)
        .values(DEFAULT_UUID, "test@test.com", "Default Organization", DEFAULT_UUID)
        .execute()
    }

    protected fun scopeColumnsExists(ctx: DSLContext): Boolean =
      ctx.fetchExists(
        DSL
          .select()
          .from("information_schema.columns")
          .where(
            DSL
              .field("table_name")
              .eq(ACTOR_DEFINITION_WORKSPACE_GRANT_TABLE)
              .and(DSL.field("column_name").eq(SCOPE_ID)),
          ),
      ) &&
        ctx.fetchExists(
          DSL
            .select()
            .from("information_schema.columns")
            .where(
              DSL
                .field("table_name")
                .eq(ACTOR_DEFINITION_WORKSPACE_GRANT_TABLE)
                .and(DSL.field("column_name").eq(SCOPE_TYPE)),
            ),
        )

    protected fun scopeColumnsMatchWorkspaceId(
      ctx: DSLContext,
      actorDefinitionId: UUID?,
      workspaceId: UUID,
    ): Boolean {
      val record =
        checkNotNull(
          ctx.fetchOne(
            DSL
              .select()
              .from(ACTOR_DEFINITION_WORKSPACE_GRANT_TABLE)
              .where(DSL.field(ACTOR_DEFINITION_ID).eq(actorDefinitionId))
              .and(DSL.field(WORKSPACE_ID).eq(workspaceId)),
          ),
        )

      return record[SCOPE_ID] == workspaceId &&
        record[SCOPE_TYPE].toString() == V0_50_5_005__AddScopeToActorDefinitionWorkspaceGrantTable.ScopeTypeEnum.workspace.literal
    }

    private fun addWorkspace(
      ctx: DSLContext,
      workspaceId: UUID,
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
          "base workspace",
          "base_workspace",
          true,
          DEFAULT_UUID,
        ).execute()
    }

    private fun addActorDefinition(
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
          "name",
          V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.source,
        ).execute()
    }
  }
}
