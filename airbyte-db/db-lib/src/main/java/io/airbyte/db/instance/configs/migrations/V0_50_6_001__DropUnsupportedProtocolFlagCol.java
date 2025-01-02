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
 * Drop unsupported_protocol_version flag from connection migration. This field has not been used
 * and is safe to remove.
 */
public class V0_50_6_001__DropUnsupportedProtocolFlagCol extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_6_001__DropUnsupportedProtocolFlagCol.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    dropUnsupportedProtocolFlagCol(ctx);
  }

  private void dropUnsupportedProtocolFlagCol(final DSLContext ctx) {
    ctx.alterTable("connection")
        .dropColumn(DSL.field("unsupported_protocol_version"))
        .execute();
  }

}
