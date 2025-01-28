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

/**
 * Add icon_url to actor_definition table.
 */
public class V0_50_33_016__AddIconUrlToActorDefinition extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_33_016__AddIconUrlToActorDefinition.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());
    addIconUrlToActorDefinition(ctx);
  }

  public static void addIconUrlToActorDefinition(final DSLContext ctx) {
    final Field<String> iconUrlColumn = DSL.field("icon_url", SQLDataType.VARCHAR(256).nullable(true));

    ctx.alterTable("actor_definition")
        .addIfNotExists(iconUrlColumn).execute();
  }

}
