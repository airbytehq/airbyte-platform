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

public class V1_1_1_027__AddOriginIndexToScopedConfiguration extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_1_027__AddOriginIndexToScopedConfiguration.class);
  private static final String SCOPED_CONFIGURATION_TABLE_NAME = "scoped_configuration";
  private static final String ORIGIN_INDEX_NAME = "scoped_configuration_origin_idx";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());

    ctx.createIndexIfNotExists(ORIGIN_INDEX_NAME)
        .on(DSL.table(SCOPED_CONFIGURATION_TABLE_NAME), DSL.field("origin"))
        .execute();
  }

}
