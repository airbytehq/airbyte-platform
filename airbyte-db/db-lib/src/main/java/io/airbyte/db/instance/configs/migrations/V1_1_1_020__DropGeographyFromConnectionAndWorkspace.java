/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V1_1_1_020__DropGeographyFromConnectionAndWorkspace extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_1_020__DropGeographyFromConnectionAndWorkspace.class);

  private static final Table<org.jooq.Record> CONNECTION = DSL.table("connection");
  private static final Table<org.jooq.Record> WORKSPACE = DSL.table("workspace");

  private static final Field<String> CONNECTION_GEOGRAPHY = DSL.field("geography",
      SQLDataType.VARCHAR);
  private static final Field<String> WORKSPACE_GEOGRAPHY_DO_NOT_USE =
      DSL.field(DSL.name("geography_DO_NOT_USE"), SQLDataType.VARCHAR);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());
    final DSLContext ctx = DSL.using(context.getConnection());
    doMigration(ctx);
  }

  static void doMigration(final DSLContext ctx) {
    LOGGER.info("Dropping 'geography' column from 'connection' table");
    ctx.alterTable(CONNECTION)
        .dropColumn(CONNECTION_GEOGRAPHY)
        .execute();

    LOGGER.info("Dropping 'geography_DO_NOT_USE' column from 'workspace' table");
    ctx.alterTable(WORKSPACE)
        .dropColumn(WORKSPACE_GEOGRAPHY_DO_NOT_USE)
        .execute();
  }

}
