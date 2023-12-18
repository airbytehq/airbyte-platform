/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_50_33_013__AddTestingValuesColumnToConnectorBuilderProject extends BaseJavaMigration {

  private static final String CONNECTOR_BUILDER_PROJECT_TABLE = "connector_builder_project";
  private static final String TESTING_VALUES_COLUMN_NAME = "testing_values";

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_33_013__AddTestingValuesColumnToConnectorBuilderProject.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());

    final Field<JSONB> testingValuesColumn = DSL.field(TESTING_VALUES_COLUMN_NAME, SQLDataType.JSONB.nullable(true));
    ctx.alterTable(CONNECTOR_BUILDER_PROJECT_TABLE)
        .addColumnIfNotExists(testingValuesColumn)
        .execute();
  }

}
