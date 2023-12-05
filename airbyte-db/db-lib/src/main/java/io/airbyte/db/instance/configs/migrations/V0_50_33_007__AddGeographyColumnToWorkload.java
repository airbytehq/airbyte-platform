/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_50_33_007__AddGeographyColumnToWorkload extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_33_007__AddGeographyColumnToWorkload.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());

    addGeographyColumnToWorkload(ctx);
  }

  private static void addGeographyColumnToWorkload(final DSLContext ctx) {
    ctx.alterTable("workload")
        .addColumnIfNotExists(DSL.field(
            "geography",
            SQLDataType.VARCHAR.nullable(false).defaultValue("AUTO")))
        .execute();
  }

}
