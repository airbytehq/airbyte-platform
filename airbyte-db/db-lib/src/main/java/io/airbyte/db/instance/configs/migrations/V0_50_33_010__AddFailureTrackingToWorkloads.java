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

// TODO: update migration description in the class name
public class V0_50_33_010__AddFailureTrackingToWorkloads extends BaseJavaMigration {

  private static final String WORKLOAD_TABLE = "workload";

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_33_010__AddFailureTrackingToWorkloads.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());

    ctx.alterTable(WORKLOAD_TABLE)
        .addColumnIfNotExists(DSL.field("termination_source", SQLDataType.VARCHAR.nullable(true)))
        .execute();
    ctx.alterTable(WORKLOAD_TABLE)
        .addColumnIfNotExists(DSL.field("termination_reason", SQLDataType.CLOB.nullable(true)))
        .execute();
  }

}
