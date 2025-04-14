/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.Record1
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.util.UUID
import java.util.stream.Collectors

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_1_1_013__PopulateDataplaneGroups : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    doMigration(ctx)
  }

  companion object {
    // DEFAULT_ORGANIZATION_ID is from io.airbyte.config.persistence.OrganizationPersistence
    val DEFAULT_ORGANIZATION_ID: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")
    private val WORKSPACE = DSL.table("workspace")
    private val DATAPLANE_GROUP = DSL.table("dataplane_group")
    private val GEOGRAPHY = DSL.field("geography", SQLDataType.VARCHAR)
    private val ID = DSL.field("id", SQLDataType.UUID)
    private val NAME = DSL.field("name", SQLDataType.VARCHAR)
    private val ENABLED = DSL.field("enabled", SQLDataType.BOOLEAN)
    private val TOMBSTONE = DSL.field("tombstone", SQLDataType.BOOLEAN)
    private val ORGANIZATION_ID = DSL.field("organization_id", SQLDataType.UUID)
    private val UPDATED_BY = DSL.field("updated_by", SQLDataType.UUID)
    private const val GEOGRAPHY_AUTO = "AUTO"
    private const val GEOGRAPHY_US = "US"
    private const val GEOGRAPHY_EU = "EU"

    @JvmStatic
    fun doMigration(ctx: DSLContext) {
      dropUpdatedByNotNullConstraint(ctx)
      addDataplaneGroupNameConstraint(ctx)
      populateDataplaneGroups(ctx)
    }

    fun dropUpdatedByNotNullConstraint(ctx: DSLContext) {
      log.info { "Dropping NOT NULL constraint on updated_by" }
      ctx
        .alterTable(DATAPLANE_GROUP)
        .alterColumn(UPDATED_BY)
        .dropNotNull()
        .execute()
    }

    fun addDataplaneGroupNameConstraint(ctx: DSLContext) {
      log.info { "Checking if constraint 'dataplane_group_name_matches_geography' exists" }

      val constraintExists =
        ctx.fetchExists(
          DSL
            .selectOne()
            .from("information_schema.table_constraints")
            .where(DSL.field("table_name").eq("dataplane_group"))
            .and(DSL.field("constraint_name").eq("dataplane_group_name_matches_geography")),
        )

      if (!constraintExists) {
        log.info { "Adding CHECK constraint to workspace.name column to allow only 'EU', 'US', or 'AUTO'" }
        ctx
          .alterTable(DATAPLANE_GROUP)
          .add(
            DSL
              .constraint("dataplane_group_name_matches_geography")
              .check(NAME.`in`(listOf(GEOGRAPHY_AUTO, GEOGRAPHY_US, GEOGRAPHY_EU))),
          ).execute()
      } else {
        log.info { "Constraint 'dataplane_group_name_matches_geography' already exists, skipping creation." }
      }
    }

    fun populateDataplaneGroups(ctx: DSLContext) {
      // Get all unique geography values
      val geographyValues =
        ctx
          .select(GEOGRAPHY)
          .from(WORKSPACE)
          .fetch()
          .stream()
          .map { r: Record1<String> -> r.get(GEOGRAPHY) }
          .collect(Collectors.toSet())

      // Get existing dataplane groups
      val existingDataplaneGroups =
        ctx
          .select(NAME)
          .from(DATAPLANE_GROUP)
          .fetch()
          .stream()
          .map { r: Record1<String> -> r.get(NAME) }
          .collect(Collectors.toSet())

      // Find missing dataplane groups
      val missingDataplaneGroups =
        geographyValues
          .stream()
          .filter { name: String -> !existingDataplaneGroups.contains(name.lowercase()) }
          .collect(Collectors.toList())

      // Insert missing dataplane groups only if they don't exist
      for (name in missingDataplaneGroups) {
        val exists =
          ctx.fetchExists(
            DSL
              .selectOne()
              .from(DATAPLANE_GROUP)
              .where(NAME.eq(name)),
          )

        if (!exists) {
          ctx
            .insertInto(DATAPLANE_GROUP)
            .columns(ID, ORGANIZATION_ID, NAME, ENABLED, TOMBSTONE)
            .values(UUID.randomUUID(), DEFAULT_ORGANIZATION_ID, name, true, false)
            .execute()
          log.info { "Inserted new dataplane group: $name" }
        } else {
          log.info { "Dataplane group '$name' already exists, skipping insertion." }
        }
      }
      // If no geography values exist, create a default dataplane group
      if (geographyValues.isEmpty() && !existingDataplaneGroups.contains(GEOGRAPHY_AUTO)) {
        ctx
          .insertInto(DATAPLANE_GROUP)
          .columns(ID, ORGANIZATION_ID, NAME, ENABLED, TOMBSTONE)
          .values(UUID.randomUUID(), DEFAULT_ORGANIZATION_ID, GEOGRAPHY_AUTO, true, false)
          .execute()
        log.info { "Inserted default dataplane" }
      }
    }
  }
}
