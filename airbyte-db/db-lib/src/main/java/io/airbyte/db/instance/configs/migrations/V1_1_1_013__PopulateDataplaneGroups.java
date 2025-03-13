/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V1_1_1_013__PopulateDataplaneGroups extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_1_013__PopulateDataplaneGroups.class);

  // DEFAULT_ORGANIZATION_ID is from io.airbyte.config.persistence.OrganizationPersistence
  public static final UUID DEFAULT_ORGANIZATION_ID = UUID.fromString("00000000-0000-0000-0000-000000000000");
  private static final Table<Record> WORKSPACE = DSL.table("workspace");
  private static final Table<Record> DATAPLANE_GROUP = DSL.table("dataplane_group");
  private static final Field<String> GEOGRAPHY = DSL.field("geography", SQLDataType.VARCHAR);
  private static final Field<UUID> ID = DSL.field("id", SQLDataType.UUID);
  private static final Field<String> NAME = DSL.field("name", SQLDataType.VARCHAR);
  private static final Field<Boolean> ENABLED = DSL.field("enabled", SQLDataType.BOOLEAN);
  private static final Field<Boolean> TOMBSTONE = DSL.field("tombstone", SQLDataType.BOOLEAN);
  private static final Field<UUID> ORGANIZATION_ID = DSL.field("organization_id", SQLDataType.UUID);
  private static final Field<UUID> UPDATED_BY = DSL.field("updated_by", SQLDataType.UUID);
  private static final String GEOGRAPHY_AUTO = "AUTO";
  private static final String GEOGRAPHY_US = "US";
  private static final String GEOGRAPHY_EU = "EU";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    doMigration(ctx);
  }

  @VisibleForTesting
  static void doMigration(final DSLContext ctx) {
    dropUpdatedByNotNullConstraint(ctx);
    addDataplaneGroupNameConstraint(ctx);
    populateDataplaneGroups(ctx);
  }

  @VisibleForTesting
  static void dropUpdatedByNotNullConstraint(final DSLContext ctx) {
    LOGGER.info("Dropping NOT NULL constraint on updated_by");
    ctx.alterTable(DATAPLANE_GROUP)
        .alterColumn(UPDATED_BY)
        .dropNotNull()
        .execute();
  }

  static void addDataplaneGroupNameConstraint(final DSLContext ctx) {
    LOGGER.info("Checking if constraint 'dataplane_group_name_matches_geography' exists");

    boolean constraintExists = ctx.fetchExists(
        DSL.selectOne()
            .from("information_schema.table_constraints")
            .where(DSL.field("table_name").eq("dataplane_group"))
            .and(DSL.field("constraint_name").eq("dataplane_group_name_matches_geography")));

    if (!constraintExists) {
      LOGGER.info("Adding CHECK constraint to workspace.name column to allow only 'EU', 'US', or 'AUTO'");
      ctx.alterTable(DATAPLANE_GROUP)
          .add(DSL.constraint("dataplane_group_name_matches_geography")
              .check(NAME.in(List.of(GEOGRAPHY_AUTO, GEOGRAPHY_US, GEOGRAPHY_EU))))
          .execute();
    } else {
      LOGGER.info("Constraint 'dataplane_group_name_matches_geography' already exists, skipping creation.");
    }
  }

  @VisibleForTesting
  static void populateDataplaneGroups(final DSLContext ctx) {
    // Get all unique geography values
    final Set<String> geographyValues = ctx.select(GEOGRAPHY)
        .from(WORKSPACE)
        .fetch()
        .stream()
        .map(r -> r.get(GEOGRAPHY))
        .collect(Collectors.toSet());

    // Get existing dataplane groups
    final Set<String> existingDataplaneGroups = ctx.select(NAME)
        .from(DATAPLANE_GROUP)
        .fetch()
        .stream()
        .map(r -> r.get(NAME))
        .collect(Collectors.toSet());

    // Find missing dataplane groups
    final List<String> missingDataplaneGroups = geographyValues.stream()
        .filter(name -> !existingDataplaneGroups.contains(name.toLowerCase()))
        .collect(Collectors.toList());

    // Insert missing dataplane groups only if they don't exist
    for (String name : missingDataplaneGroups) {
      boolean exists = ctx.fetchExists(
          DSL.selectOne()
              .from(DATAPLANE_GROUP)
              .where(NAME.eq(name)));

      if (!exists) {
        ctx.insertInto(DATAPLANE_GROUP)
            .columns(ID, ORGANIZATION_ID, NAME, ENABLED, TOMBSTONE)
            .values(UUID.randomUUID(), DEFAULT_ORGANIZATION_ID, name, true, false)
            .execute();
        LOGGER.info("Inserted new dataplane group: {}", name);
      } else {
        LOGGER.info("Dataplane group '{}' already exists, skipping insertion.", name);
      }
    }
    // If no geography values exist, create a default dataplane group
    if (geographyValues.isEmpty() && !existingDataplaneGroups.contains(GEOGRAPHY_AUTO)) {
      ctx.insertInto(DATAPLANE_GROUP)
          .columns(ID, ORGANIZATION_ID, NAME, ENABLED, TOMBSTONE)
          .values(UUID.randomUUID(), DEFAULT_ORGANIZATION_ID, GEOGRAPHY_AUTO, true, false)
          .execute();
      LOGGER.info("Inserted default dataplane");
    }
  }

}
