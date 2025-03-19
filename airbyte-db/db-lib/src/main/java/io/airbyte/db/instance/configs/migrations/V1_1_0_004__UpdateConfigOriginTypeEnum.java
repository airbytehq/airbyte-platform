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

public class V1_1_0_004__UpdateConfigOriginTypeEnum extends BaseJavaMigration {

  private static final String SCOPED_CONFIGURATION_TABLE = "scoped_configuration";
  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_0_004__UpdateConfigOriginTypeEnum.class);
  private static final String ORIGIN_TYPE_COLUMN = "origin_type";
  private static final String CONFIG_ORIGIN_TYPE = "config_origin_type";
  private static final String RELEASE_CANDIDATE = "release_candidate";
  private static final String CONNECTOR_ROLLOUT = "connector_rollout";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    runMigration(ctx);
  }

  public static void runMigration(final DSLContext ctx) {
    ctx.alterType(CONFIG_ORIGIN_TYPE).renameValue(RELEASE_CANDIDATE).to(CONNECTOR_ROLLOUT).execute();
    LOGGER.info(
        "Updated from '{}' to '{}' in table '{}' column '{}'",
        RELEASE_CANDIDATE, CONNECTOR_ROLLOUT, SCOPED_CONFIGURATION_TABLE, ORIGIN_TYPE_COLUMN);
  }

}
