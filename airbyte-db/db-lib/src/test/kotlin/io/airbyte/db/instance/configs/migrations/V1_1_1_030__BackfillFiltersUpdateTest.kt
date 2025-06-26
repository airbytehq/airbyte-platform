/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.airbyte.config.AttributeName
import io.airbyte.config.ConnectorRolloutFilters
import io.airbyte.config.CustomerTier
import io.airbyte.config.CustomerTierFilter
import io.airbyte.config.Operator
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
internal class V1_1_1_030__BackfillFiltersUpdateTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      FlywayFactory.create(
        dataSource,
        "V1_1_1_031__FixBackfilledConnectorRolloutFilters",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )

    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)
    val previousMigration: BaseJavaMigration = V1_1_1_029__DropAndRecreateRolloutIndexWithTag()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun `migration drops bad filters and backfills with correct format`() {
    val ctx = dslContext!!
    ctx.execute("ALTER TABLE connector_rollout DROP CONSTRAINT IF EXISTS fk_actor_definition_id")
    ctx.execute("ALTER TABLE connector_rollout DROP CONSTRAINT IF EXISTS fk_release_candidate_version_id")

    val rolloutId = UUID.randomUUID()

    // Insert row with legacy-format filters
    val badJson =
      """
      {
        "organization_customer_attributes": [
          {
            "name": "tier",
            "operator": "in",
            "values": ["${CustomerTier.TIER_2}"]
          }
        ]
      }
      """.trimIndent()

    ctx
      .insertInto(CONNECTOR_ROLLOUT_TABLE)
      .set(ID_FIELD, rolloutId)
      .set(DSL.field("actor_definition_id", UUID::class.java), UUID.randomUUID())
      .set(DSL.field("release_candidate_version_id", UUID::class.java), UUID.randomUUID())
      .set(DSL.field("state", String::class.java), "in_progress")
      .set(DSL.field("has_breaking_changes", Boolean::class.java), false)
      .set(DSL.field("created_at", java.sql.Timestamp::class.java), java.sql.Timestamp(System.currentTimeMillis()))
      .set(DSL.field("updated_at", java.sql.Timestamp::class.java), java.sql.Timestamp(System.currentTimeMillis()))
      .set(FILTERS_FIELD, DSL.inline(badJson, SQLDataType.JSONB))
      .execute()

    // Run the migration
    V1_1_1_030__BackfillFiltersUpdate().doMigration(ctx)

    val result =
      ctx
        .select(ID_FIELD, FILTERS_FIELD)
        .from(CONNECTOR_ROLLOUT_TABLE)
        .where(ID_FIELD.eq(rolloutId))
        .fetchOne()

    assertNotNull(result)
    val fixedJson = result?.get(FILTERS_FIELD)?.data()
    assertNotNull(fixedJson)

    // Validate the shape of the new JSON
    assertTrue(fixedJson!!.contains("customerTierFilters"))
    assertTrue(fixedJson.contains("TIER"))
    assertTrue(fixedJson.contains("IN"))
    assertTrue(fixedJson.contains("value"))
    assertTrue(fixedJson.contains(CustomerTier.TIER_2.name))

    val mapper = jacksonObjectMapper()
    val jsonNode: JsonNode = mapper.readTree(fixedJson)
    val filtersArray = jsonNode["customerTierFilters"]

    // Verify we can cast filters to Config
    val filters =
      ConnectorRolloutFilters(
        customerTierFilters =
          filtersArray.map {
            CustomerTierFilter(
              name = AttributeName.valueOf(it["name"].asText()),
              operator = Operator.valueOf(it["operator"].asText()),
              value = it["value"].map { v -> CustomerTier.valueOf(v.asText()) },
            )
          },
      )
    assertEquals(1, filters.customerTierFilters.size)
    assertEquals(
      "TIER",
      filters.customerTierFilters
        .first()
        .name
        .toString(),
    )
    assertEquals(
      "IN",
      filters.customerTierFilters
        .first()
        .operator
        .toString(),
    )
    assertEquals(
      "TIER_2",
      filters.customerTierFilters
        .first()
        .value
        .first()
        .toString(),
    )
  }

  companion object {
    val CONNECTOR_ROLLOUT_TABLE: Table<*> = DSL.table("connector_rollout")
    val ID_FIELD: Field<UUID> = DSL.field("id", SQLDataType.UUID)
    val FILTERS_FIELD: Field<org.jooq.JSONB> = DSL.field("filters", SQLDataType.JSONB)
  }
}
