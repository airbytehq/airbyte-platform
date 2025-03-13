/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import com.google.common.annotations.VisibleForTesting;
import java.util.UUID;
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

public class V1_1_1_014__AddDataplaneGroupIdToWorkspace extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_1_014__AddDataplaneGroupIdToWorkspace.class);

  private static final Table<Record> WORKSPACE = DSL.table("workspace");
  private static final Table<Record> DATAPLANE_GROUP = DSL.table("dataplane_group");
  private static final Field<String> GEOGRAPHY = DSL.field("geography", SQLDataType.VARCHAR);
  private static final Field<UUID> DATAPLANE_GROUP_ID = DSL.field("dataplane_group_id", SQLDataType.UUID.nullable(false));
  private static final Field<UUID> WORKSPACE_DATAPLANE_GROUP_ID = DSL.field("dataplane_group_id", SQLDataType.UUID.nullable(false));
  private static final Field<UUID> DATAPLANE_GROUP_PK = DSL.field("dataplane_group.id", SQLDataType.UUID.nullable(false));
  private static final Field<String> WORKSPACE_GEOGRAPHY = DSL.field("geography", SQLDataType.VARCHAR.nullable(false));
  private static final Field<String> DATAPLANE_GROUP_NAME = DSL.field("dataplane_group.name", SQLDataType.VARCHAR.nullable(false));

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());
    final DSLContext ctx = DSL.using(context.getConnection());
    doMigration(ctx);
  }

  @VisibleForTesting
  static void doMigration(final DSLContext ctx) {
    addColumn(ctx);
    populateDataplaneGroupIds(ctx);
    addNotNullConstraint(ctx);
    updateGeographyColumnDropNotNullConstraintAndRename(ctx);
  }

  @VisibleForTesting
  static void addColumn(final DSLContext ctx) {
    LOGGER.info("Adding column dataplane_group_id to workspace table");
    ctx.alterTable(WORKSPACE)
        .addColumnIfNotExists(DATAPLANE_GROUP_ID, SQLDataType.UUID.nullable(true))
        .execute();
  }

  @VisibleForTesting
  static void populateDataplaneGroupIds(final DSLContext ctx) {
    LOGGER.info("Updating workspaces with dataplane_group_id");
    // Update workspace table with corresponding dataplane_group_id
    ctx.update(WORKSPACE)
        .set(WORKSPACE_DATAPLANE_GROUP_ID, DATAPLANE_GROUP_PK)
        .from(DATAPLANE_GROUP)
        .where(WORKSPACE_GEOGRAPHY.cast(SQLDataType.VARCHAR).eq(DATAPLANE_GROUP_NAME))
        .execute();
  }

  @VisibleForTesting
  static void addNotNullConstraint(final DSLContext ctx) {
    LOGGER.info("Adding NOT NULL constraint to dataplane_group_id");
    ctx.alterTable(WORKSPACE)
        .alterColumn(DATAPLANE_GROUP_ID)
        .setNotNull()
        .execute();
  }

  @VisibleForTesting
  static void updateGeographyColumnDropNotNullConstraintAndRename(final DSLContext ctx) {
    LOGGER.info("Dropping NOT NULL constraint on geography");
    ctx.alterTable(WORKSPACE)
        .alterColumn(GEOGRAPHY)
        .dropNotNull()
        .execute();
    LOGGER.info("Renaming geography column to geography_DO_NOT_USE in workspace table");
    ctx.alterTable(WORKSPACE)
        .renameColumn(GEOGRAPHY).to("geography_DO_NOT_USE")
        .execute();
  }

}
