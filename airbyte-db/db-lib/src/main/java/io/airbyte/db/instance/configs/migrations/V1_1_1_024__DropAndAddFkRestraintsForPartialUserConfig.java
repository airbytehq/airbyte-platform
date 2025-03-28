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

public class V1_1_1_024__DropAndAddFkRestraintsForPartialUserConfig extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_1_024__DropAndAddFkRestraintsForPartialUserConfig.class);

  static final String PARTIAL_USER_CONFIG_TABLE_NAME = "partial_user_config";
  static final String PARTIAL_USER_CONFIG_WORKSPACE_ID_FK = "partial_user_config_workspace_id_fkey";
  static final String WORKSPACE_ID_FIELD = "workspace_id";
  static final String WORKSPACE_TABLE = "workspace";
  static final String ID_FIELD = "id";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());
    final DSLContext ctx = DSL.using(context.getConnection());
    doMigration(ctx);
  }

  static void doMigration(final DSLContext ctx) {
    ctx.alterTable(PARTIAL_USER_CONFIG_TABLE_NAME)
        .dropIfExists(DSL.constraint(PARTIAL_USER_CONFIG_WORKSPACE_ID_FK))
        .execute();

    ctx.alterTable(PARTIAL_USER_CONFIG_TABLE_NAME)
        .add(DSL.constraint(PARTIAL_USER_CONFIG_WORKSPACE_ID_FK)
            .foreignKey(WORKSPACE_ID_FIELD)
            .references(WORKSPACE_TABLE, ID_FIELD)
            .onDeleteCascade())
        .execute();

  }

}
