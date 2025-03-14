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

/**
 * Adds airbyte_managed boolean column to SecretConfig table.
 */
public class V1_1_1_015__AddAirbyteManagedBooleanToSecretConfigTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_1_015__AddAirbyteManagedBooleanToSecretConfigTable.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    addAirbyteManagedColumn(ctx);
  }

  // Note that the SecretConfig table is currently empty, so we can add a non-nullable column
  // without providing a default value. We want writers to explicitly set this column's value
  // for all rows, so a default value would be inappropriate.
  static void addAirbyteManagedColumn(final DSLContext ctx) {
    ctx.alterTable("secret_config")
        .addColumn("airbyte_managed", SQLDataType.BOOLEAN.nullable(false))
        .execute();
  }

}
