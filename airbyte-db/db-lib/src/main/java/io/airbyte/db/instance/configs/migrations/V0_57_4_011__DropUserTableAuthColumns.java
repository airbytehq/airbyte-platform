/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_57_4_011__DropUserTableAuthColumns extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_57_4_011__DropUserTableAuthColumns.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    dropAuthColumns(ctx);
  }

  static void dropAuthColumns(final DSLContext ctx) {
    ctx.alterTable("user")
        .dropColumnIfExists("auth_provider")
        .execute();

    ctx.alterTable("user")
        .dropColumnIfExists("auth_user_id")
        .execute();
  }

}
