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

public class V1_1_0_006__MakeResourceColumnsNullableScopedConfiguration extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_0_006__MakeResourceColumnsNullableScopedConfiguration.class);
  private static final String SCOPED_CONFIGURATION = "scoped_configuration";
  private static final String RESOURCE_TYPE = "resource_type";
  private static final String RESOURCE_ID = "resource_id";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());
    runMigration(ctx);
  }

  public static void runMigration(final DSLContext ctx) {
    ctx.alterTable(SCOPED_CONFIGURATION)
        .alter(DSL.field(RESOURCE_ID)).dropNotNull()
        .execute();

    ctx.alterTable(SCOPED_CONFIGURATION)
        .alter(DSL.field(RESOURCE_TYPE)).dropNotNull()
        .execute();
  }

}
