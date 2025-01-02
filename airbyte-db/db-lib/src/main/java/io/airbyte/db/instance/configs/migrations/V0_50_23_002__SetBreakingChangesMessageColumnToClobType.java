/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import com.google.common.annotations.VisibleForTesting;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Changes the message column type of actor_definition_breaking_change to CLOB, which will set\ it
 * to 'text' in the db. We want to be able to handle large messages.
 */
public class V0_50_23_002__SetBreakingChangesMessageColumnToClobType extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_23_002__SetBreakingChangesMessageColumnToClobType.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    alterMessageColumnType(ctx);
  }

  @VisibleForTesting
  static void alterMessageColumnType(final DSLContext ctx) {
    ctx.alterTable("actor_definition_breaking_change")
        .alter("message").set(SQLDataType.CLOB).execute();
  }

}
