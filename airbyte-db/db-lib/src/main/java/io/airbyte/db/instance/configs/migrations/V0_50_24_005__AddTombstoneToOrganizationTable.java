/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_50_24_005__AddTombstoneToOrganizationTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_24_005__AddTombstoneToOrganizationTable.class);
  private static final String ORGANIZATION_TABLE = "organization";
  private static final String TOMBSTONE_COLUMN = "tombstone";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());
    addTombstoneColumn(ctx);

    LOGGER.info("Migration finished!");
  }

  static void addTombstoneColumn(final DSLContext ctx) {
    ctx.alterTable(ORGANIZATION_TABLE)
        .addColumnIfNotExists(DSL.field(TOMBSTONE_COLUMN, SQLDataType.BOOLEAN.nullable(false).defaultValue(false)))
        .execute();
  }

}
