/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.migrations.V0_50_23_002__SetBreakingChangesMessageColumnToClobType.Companion.alterMessageColumnType
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.exception.DataAccessException
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.sql.Date
import java.sql.Timestamp
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_50_23_002__SetBreakingChangesMessageColumnToClobTypeTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_50_23_002__SetBreakingChangesMessageColumnToClobTypeTest.java",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)

    val previousMigration: BaseJavaMigration = V0_50_21_001__BackfillActorDefaultVersionAndSetNonNull()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun testInsertThrowsBeforeMigration() {
    val ctx = dslContext!!
    insertActorDefinitionDependency(ctx)
    val exception: Throwable =
      Assertions.assertThrows(
        DataAccessException::class.java,
      ) { insertBreakingChange(ctx) }
    Assertions.assertTrue(exception.message!!.contains("value too long for type character varying(256)"))
  }

  @Test
  fun testInsertSucceedsAfterMigration() {
    val ctx = dslContext!!
    insertActorDefinitionDependency(ctx)
    alterMessageColumnType(ctx)
    Assertions.assertDoesNotThrow {
      insertBreakingChange(
        ctx,
      )
    }
  }

  companion object {
    private val ACTOR_DEFINITION_BREAKING_CHANGE = DSL.table("actor_definition_breaking_change")
    private val ACTOR_DEFINITION = DSL.table("actor_definition")
    private val ACTOR_DEFINITION_ID: UUID = UUID.randomUUID()

    private fun insertActorDefinitionDependency(ctx: DSLContext) {
      ctx
        .insertInto(ACTOR_DEFINITION)
        .columns(
          DSL.field("id"),
          DSL.field("name"),
          DSL.field("actor_type"),
        ).values(
          ACTOR_DEFINITION_ID,
          "source def name",
          V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.source,
        ).onConflict(
          DSL.field("id"),
        ).doNothing()
        .execute()
    }

    private fun insertBreakingChange(ctx: DSLContext) {
      val message =
        "This version introduces [Destinations V2](https://docs.airbyte.com/release_notes/upgrading_to_destinations_v2/#what-is-destinations-v2), which provides better error handling, incremental delivery of data for large syncs, and improved final table structures. To review the breaking changes, and how to upgrade, see [here](https://docs.airbyte.com/release_notes/upgrading_to_destinations_v2/#quick-start-to-upgrading). These changes will likely require updates to downstream dbt / SQL models, which we walk through [here](https://docs.airbyte.com/release_notes/upgrading_to_destinations_v2/#updating-downstream-transformations). Selecting `Upgrade` will upgrade **all** connections using this destination at their next sync. You can manually sync existing connections prior to the next scheduled sync to start the upgrade early."

      ctx
        .insertInto(ACTOR_DEFINITION_BREAKING_CHANGE)
        .columns(
          DSL.field("actor_definition_id"),
          DSL.field("version"),
          DSL.field("upgrade_deadline"),
          DSL.field("message"),
          DSL.field("migration_documentation_url"),
          DSL.field("created_at"),
          DSL.field("updated_at"),
        ).values(
          ACTOR_DEFINITION_ID,
          "3.0.0",
          Date.valueOf("2023-11-01"),
          message,
          "https://docs.airbyte.com/integrations/destinations/snowflake-migrations#3.0.0",
          Timestamp.valueOf("2023-08-25 16:33:42.701943875"),
          Timestamp.valueOf("2023-08-25 16:33:42.701943875"),
        ).onConflict(
          DSL.field("actor_definition_id"),
          DSL.field("version"),
        ).doUpdate()
        .set(DSL.field("upgrade_deadline"), Date.valueOf("2023-11-01"))
        .set(DSL.field("message"), message)
        .set(
          DSL.field("migration_documentation_url"),
          "https://docs.airbyte.com/integrations/destinations/snowflake-migrations#3.0.0",
        ).set(DSL.field("updated_at"), Timestamp.valueOf("2023-08-25 16:33:42.701943875"))
        .execute()
    }
  }
}
