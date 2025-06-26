/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.config.CustomerTier
import io.airbyte.db.factory.FlywayFactory
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.Field
import org.jooq.Table
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V1_1_1_028__AddFiltersToConnectorRolloutTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      FlywayFactory.create(
        dataSource,
        "V1_1_1_028__AddFiltersToConnectorRollout",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )

    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)
    val previousMigration: BaseJavaMigration = V1_1_1_026__AddActorIdToPartialUserConfig()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun `filters column exists and is backfilled`() {
    val ctx = dslContext!!
    ctx.execute("ALTER TABLE connector_rollout DROP CONSTRAINT IF EXISTS fk_actor_definition_id")
    ctx.execute("ALTER TABLE connector_rollout DROP CONSTRAINT IF EXISTS fk_release_candidate_version_id")

    val rolloutId = UUID.randomUUID()

    // Insert a rollout row without the filters column populated
    ctx
      .insertInto(CONNECTOR_ROLLOUT_TABLE)
      .set(ID_FIELD, rolloutId)
      .set(DSL.field("actor_definition_id", UUID::class.java), UUID.randomUUID())
      .set(DSL.field("release_candidate_version_id", UUID::class.java), UUID.randomUUID())
      .set(DSL.field("state", String::class.java), "in_progress")
      .set(DSL.field("has_breaking_changes", Boolean::class.java), false)
      .set(DSL.field("created_at", java.sql.Timestamp::class.java), java.sql.Timestamp(System.currentTimeMillis()))
      .set(DSL.field("updated_at", java.sql.Timestamp::class.java), java.sql.Timestamp(System.currentTimeMillis()))
      .execute()

    V1_1_1_028__AddFiltersToConnectorRollout.doMigration(ctx)

    val result =
      ctx
        .select(ID_FIELD, FILTERS_FIELD)
        .from(CONNECTOR_ROLLOUT_TABLE)
        .where(ID_FIELD.eq(rolloutId))
        .fetchOne()

    assertNotNull(result)
    assertEquals(rolloutId, result?.get(ID_FIELD))
    val filtersJson = result?.get(FILTERS_FIELD)?.data()
    assertNotNull(filtersJson)
    assertTrue(filtersJson!!.contains("organization_customer_attributes"))
    assertTrue(filtersJson.contains("tier"))
    assertTrue(filtersJson.contains(CustomerTier.TIER_2.name))
  }

  companion object {
    val CONNECTOR_ROLLOUT_TABLE: Table<*> = DSL.table("connector_rollout")
    val ID_FIELD: Field<UUID> = DSL.field("id", SQLDataType.UUID)
    val FILTERS_FIELD: Field<org.jooq.JSONB> = DSL.field("filters", SQLDataType.JSONB)
  }
}
