/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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

public class V0_57_4_001__AddRefreshSupport extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_57_4_001__AddRefreshSupport.class);

  static final String ACTOR_DEFINITION = "actor_definition";

  private static final Field<Boolean> supportRefreshes = DSL.field("support_refreshes", SQLDataType.BOOLEAN.nullable(false).defaultValue(false));

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());
    final DSLContext ctx = DSL.using(context.getConnection());
    updateSupportRefreshes(ctx);
  }

  static void updateSupportRefreshes(final DSLContext ctx) {
    ctx.alterTable(ACTOR_DEFINITION).addColumn(supportRefreshes).execute();
  }

}
