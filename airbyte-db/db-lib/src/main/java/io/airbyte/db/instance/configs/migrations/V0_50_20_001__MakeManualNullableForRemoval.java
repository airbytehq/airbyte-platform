/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static io.airbyte.db.instance.DatabaseConstants.CONNECTION_TABLE;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A migration to make the "manual" column on the connection table nullable.
 *
 * This is so we can stop writing to it, and then remove it altogether.
 */
public class V0_50_20_001__MakeManualNullableForRemoval extends BaseJavaMigration {

  private static final Field<Boolean> MANUAL_COLUMN = DSL.field("manual", SQLDataType.BOOLEAN);

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_20_001__MakeManualNullableForRemoval.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    makeManualNullable(ctx);
  }

  private static void makeManualNullable(final DSLContext context) {
    context.alterTable(DSL.table(CONNECTION_TABLE)).alter(MANUAL_COLUMN).dropNotNull().execute();
  }

}
