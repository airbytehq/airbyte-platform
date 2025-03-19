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

/**
 * Migration to drop the column `default_version_id` from the `actor` table. This column is no
 * longer needed as breaking changes are now handled via scoped_configuration entries.
 */
public class V0_57_4_003__DropActorDefaultVersionIdCol extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_57_4_003__DropActorDefaultVersionIdCol.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    dropActorDefaultVersionIdCol(ctx);
  }

  private static void dropActorDefaultVersionIdCol(final DSLContext ctx) {
    ctx.alterTable(DSL.table("actor"))
        .dropColumnIfExists("default_version_id")
        .execute();
  }

}
