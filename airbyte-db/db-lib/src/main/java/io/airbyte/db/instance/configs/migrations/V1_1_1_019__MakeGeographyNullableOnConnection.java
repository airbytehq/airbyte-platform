/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

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

public class V1_1_1_019__MakeGeographyNullableOnConnection extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_1_019__MakeGeographyNullableOnConnection.class);
  private static final Table<Record> CONNECTION = DSL.table("connection");
  private static final Field<String> CONNECTION_GEOGRAPHY = DSL.field("geography", SQLDataType.VARCHAR.nullable(true));

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());
    final DSLContext ctx = DSL.using(context.getConnection());
    doMigration(ctx);
  }

  static void doMigration(final DSLContext ctx) {
    LOGGER.info("Making 'geography' column in 'connection' table nullable");
    ctx.alterTable(CONNECTION)
        .alterColumn(CONNECTION_GEOGRAPHY)
        .dropNotNull()
        .execute();
  }

}
