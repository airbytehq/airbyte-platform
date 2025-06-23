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
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V1_1_0_005__UpdateConnectorRolloutStateEnumTest : AbstractConfigsDatabaseTest() {
  private var configsDbMigrator: ConfigsDatabaseMigrator? = null

  @BeforeEach
  fun setUp() {
    val flyway =
      create(
        dataSource,
        "V1_1_0_005__UpdateConnectorRolloutStateEnumTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)

    // Initialize the database with migrations up to, but not including, our target migration
    val previousMigration: BaseJavaMigration = V1_1_0_004__UpdateConfigOriginTypeEnum()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator!!, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun testMigration() {
    val ctx = dslContext!!
    ctx.alterTable(CONNECTOR_ROLLOUT_TABLE).dropConstraintIfExists("fk_actor_definition_id").execute()
    ctx.alterTable(CONNECTOR_ROLLOUT_TABLE).dropConstraintIfExists("fk_initial_version_id").execute()
    ctx.alterTable(CONNECTOR_ROLLOUT_TABLE).dropConstraintIfExists("fk_release_candidate_version_id").execute()
    ctx.alterTable(CONNECTOR_ROLLOUT_TABLE).dropConstraintIfExists("fk_updated_by").execute()

    // Insert a record with state CANCELED_ROLLED_BACK before migration
    val canceledRolledBackId = insertRecordWithStateCanceledRolledBack(ctx)
    Assertions.assertNotNull(canceledRolledBackId)

    V1_1_0_005__UpdateConnectorRolloutStateEnum.runMigration(ctx)

    verifyAllRecordsUpdated(ctx)
    Assertions.assertThrows(
      Exception::class.java,
    ) { insertRecordWithStateCanceledRolledBack(ctx) }
    val connectorRolloutId = insertRecordWithStateCanceled(ctx)
    Assertions.assertNotNull(connectorRolloutId)
  }

  private fun insertRecordWithStateCanceledRolledBack(ctx: DSLContext): UUID {
    val rolloutId = UUID.randomUUID()
    ctx
      .insertInto(DSL.table(CONNECTOR_ROLLOUT_TABLE))
      .columns(
        DSL.field("id"),
        DSL.field("actor_definition_id"),
        DSL.field("release_candidate_version_id"),
        DSL.field("initial_version_id"),
        DSL.field("state"),
        DSL.field("initial_rollout_pct"),
        DSL.field("current_target_rollout_pct"),
        DSL.field("final_target_rollout_pct"),
        DSL.field("has_breaking_changes"),
        DSL.field("max_step_wait_time_mins"),
        DSL.field("updated_by"),
        DSL.field("created_at"),
        DSL.field("updated_at"),
        DSL.field("completed_at"),
        DSL.field("expires_at"),
        DSL.field("error_msg"),
        DSL.field("failed_reason"),
        DSL.field("rollout_strategy"),
      ).values(
        rolloutId,
        UUID.randomUUID(),
        UUID.randomUUID(),
        UUID.randomUUID(),
        DSL.field("?::connector_rollout_state_type", "canceled_rolled_back"),
        0,
        0,
        0,
        false,
        0,
        UUID.randomUUID(),
        OffsetDateTime.now(),
        OffsetDateTime.now(),
        OffsetDateTime.now(),
        OffsetDateTime.now(),
        "",
        "",
        DSL.field("?::connector_rollout_strategy_type", "manual"),
      ).execute()
    return rolloutId
  }

  private fun insertRecordWithStateCanceled(ctx: DSLContext): UUID {
    val rolloutId = UUID.randomUUID()
    ctx
      .insertInto(DSL.table(CONNECTOR_ROLLOUT_TABLE))
      .columns(
        DSL.field("id"),
        DSL.field("actor_definition_id"),
        DSL.field("release_candidate_version_id"),
        DSL.field("initial_version_id"),
        DSL.field("state"),
        DSL.field("initial_rollout_pct"),
        DSL.field("current_target_rollout_pct"),
        DSL.field("final_target_rollout_pct"),
        DSL.field("has_breaking_changes"),
        DSL.field("max_step_wait_time_mins"),
        DSL.field("updated_by"),
        DSL.field("created_at"),
        DSL.field("updated_at"),
        DSL.field("completed_at"),
        DSL.field("expires_at"),
        DSL.field("error_msg"),
        DSL.field("failed_reason"),
        DSL.field("rollout_strategy"),
      ).values(
        rolloutId,
        UUID.randomUUID(),
        UUID.randomUUID(),
        UUID.randomUUID(),
        DSL.field("?::connector_rollout_state_type", "canceled"),
        0,
        0,
        0,
        false,
        0,
        UUID.randomUUID(),
        OffsetDateTime.now(),
        OffsetDateTime.now(),
        OffsetDateTime.now(),
        OffsetDateTime.now(),
        "",
        "",
        DSL.field("?::connector_rollout_strategy_type", "manual"),
      ).execute()
    return rolloutId
  }

  private fun verifyAllRecordsUpdated(ctx: DSLContext) {
    var count =
      ctx
        .selectCount()
        .from(CONNECTOR_ROLLOUT_TABLE)
        .where(
          DSL
            .field(STATE_COLUMN)
            .cast(
              String::class.java,
            ).eq(CANCELED_ROLLED_BACK),
        ).fetchOne(0, Int::class.javaPrimitiveType)!!
    Assertions.assertEquals(0, count, "There should be no CANCELED_ROLLED_BACK records after migration")

    count =
      ctx
        .selectCount()
        .from(CONNECTOR_ROLLOUT_TABLE)
        .where(
          DSL
            .field(STATE_COLUMN)
            .cast(
              String::class.java,
            ).eq(CANCELED),
        ).fetchOne(0, Int::class.javaPrimitiveType)!!
    Assertions.assertTrue(count > 0, "There should be at least one CANCELED record after migration")
  }

  companion object {
    private const val CONNECTOR_ROLLOUT_TABLE = "connector_rollout"
    private const val STATE_COLUMN = "state"
    private const val CANCELED_ROLLED_BACK = "canceled_rolled_back"
    private const val CANCELED = "canceled"
  }
}
