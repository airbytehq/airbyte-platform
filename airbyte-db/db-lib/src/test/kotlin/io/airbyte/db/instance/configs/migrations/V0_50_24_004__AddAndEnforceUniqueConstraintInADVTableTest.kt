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
import org.jooq.Field
import org.jooq.JSONB
import org.jooq.Record
import org.jooq.Table
import org.jooq.exception.DataAccessException
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_50_24_004__AddAndEnforceUniqueConstraintInADVTableTest : AbstractConfigsDatabaseTest() {
  val actorDefinitionVersion: Table<Record> = DSL.table("actor_definition_version")
  val actorDefinitionId: Field<UUID> = DSL.field("actor_definition_id", UUID::class.java)
  val dockerImageTag: Field<String> = DSL.field("docker_image_tag", String::class.java)

  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_50_24_004__AddAndEnforceUniqueConstraintInADVTableTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V0_50_24_003__AddPbaAndOrgBillingColumnsToOrganizationTable()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun testMigrate() {
    val ctx = getDslContext()

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;")

    val actorDefinitionId1 = UUID.randomUUID()
    val actorDefinitionId2 = UUID.randomUUID()
    val actorDefinitionId3 = UUID.randomUUID()

    // Set up a state with multiple versions and some duplicates
    insertAdv(ctx, actorDefinitionId1, "1.0.0")
    insertAdv(ctx, actorDefinitionId1, "1.0.0")
    insertAdv(ctx, actorDefinitionId1, "1.0.0")

    insertAdv(ctx, actorDefinitionId2, "2.0.0")
    insertAdv(ctx, actorDefinitionId2, "2.0.1")

    insertAdv(ctx, actorDefinitionId3, "3.0.0")
    insertAdv(ctx, actorDefinitionId3, "3.0.0")
    insertAdv(ctx, actorDefinitionId3, "3.0.0")
    insertAdv(ctx, actorDefinitionId3, "3.0.1")
    insertAdv(ctx, actorDefinitionId3, "3.0.2")

    // Initial assertions
    assertAdvCount(ctx, actorDefinitionId1, 3)
    assertAdvCount(ctx, actorDefinitionId2, 2)
    assertAdvCount(ctx, actorDefinitionId3, 5)

    assertAdvTagCount(ctx, actorDefinitionId1, "1.0.0", 3)
    assertAdvTagCount(ctx, actorDefinitionId2, "2.0.0", 1)
    assertAdvTagCount(ctx, actorDefinitionId2, "2.0.1", 1)
    assertAdvTagCount(ctx, actorDefinitionId3, "3.0.0", 3)
    assertAdvTagCount(ctx, actorDefinitionId3, "3.0.1", 1)
    assertAdvTagCount(ctx, actorDefinitionId3, "3.0.2", 1)

    // Run migration
    V0_50_24_004__AddAndEnforceUniqueConstraintInADVTable.migrate(ctx)

    // Assert duplicate rows were dropped
    assertAdvCount(ctx, actorDefinitionId1, 1)
    assertAdvCount(ctx, actorDefinitionId2, 2)
    assertAdvCount(ctx, actorDefinitionId3, 3)

    assertAdvTagCount(ctx, actorDefinitionId1, "1.0.0", 1)
    assertAdvTagCount(ctx, actorDefinitionId2, "2.0.0", 1)
    assertAdvTagCount(ctx, actorDefinitionId2, "2.0.1", 1)
    assertAdvTagCount(ctx, actorDefinitionId3, "3.0.0", 1)
    assertAdvTagCount(ctx, actorDefinitionId3, "3.0.1", 1)

    // Attempting to re-insert an existing row should now fail
    Assertions.assertThrows(
      DataAccessException::class.java,
    ) {
      insertAdv(
        ctx,
        actorDefinitionId1,
        "1.0.0",
      )
    }
  }

  private fun assertAdvCount(
    ctx: DSLContext,
    actorDefinitionId: UUID,
    expectedCount: Int,
  ) {
    val actualCount =
      ctx
        .select()
        .from(actorDefinitionVersion)
        .where(this.actorDefinitionId.eq(actorDefinitionId))
        .fetch()
        .size
    Assertions.assertEquals(expectedCount, actualCount)
  }

  private fun assertAdvTagCount(
    ctx: DSLContext,
    actorDefinitionId: UUID,
    dockerImageTag: String,
    expectedCount: Int,
  ) {
    val actualCount =
      ctx
        .select()
        .from(actorDefinitionVersion)
        .where(this.actorDefinitionId.eq(actorDefinitionId))
        .and(this.dockerImageTag.eq(dockerImageTag))
        .fetch()
        .size
    Assertions.assertEquals(expectedCount, actualCount)
  }

  companion object {
    private val spec = JSONB.valueOf("{\"some\": \"value\"}")

    private fun insertAdv(
      ctx: DSLContext,
      actorDefinitionId: UUID,
      dockerImageTag: String,
    ) {
      ctx
        .insertInto(DSL.table("actor_definition_version"))
        .columns(
          DSL.field("id"),
          DSL.field("actor_definition_id"),
          DSL.field("documentation_url"),
          DSL.field("docker_repository"),
          DSL.field("docker_image_tag"),
          DSL.field("spec"),
          DSL.field("protocol_version"),
          DSL.field("allowed_hosts"),
          DSL.field("release_stage"),
          DSL.field("support_state"),
          DSL.field("support_level"),
        ).values(
          UUID.randomUUID(),
          actorDefinitionId,
          "https://docs.airbyte.com/integrations/sources/salesforce",
          "airbyte/source-salesforce",
          dockerImageTag,
          spec,
          "0.2.0",
          JSONB.valueOf("{\"hosts\": [\"*.salesforce.com\"]}"),
          V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.ReleaseStage.generally_available,
          V0_50_7_001__AddSupportStateToActorDefinitionVersion.SupportState.supported,
          V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.SupportLevel.certified,
        ).execute()
    }
  }
}
