/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_50_33_006__AddInputPayloadAndLogPathColumnsToWorkload extends BaseJavaMigration {

  private static final String TABLE = "workload";
  private static final String PAYLOAD_COLUMN_NAME = "input_payload";
  private static final String LOG_PATH_COLUMN_NAME = "log_path";

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_33_006__AddInputPayloadAndLogPathColumnsToWorkload.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());

    final Field<String> payloadColumn = DSL.field(PAYLOAD_COLUMN_NAME, SQLDataType.CLOB.nullable(false));
    ctx.alterTable(TABLE)
        .addColumnIfNotExists(payloadColumn)
        .execute();

    final Field<String> logPathColumn = DSL.field(LOG_PATH_COLUMN_NAME, SQLDataType.CLOB.nullable(false));
    ctx.alterTable(TABLE)
        .addColumnIfNotExists(logPathColumn)
        .execute();
  }

}
