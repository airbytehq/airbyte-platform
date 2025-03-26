/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drop the notification column and re-add it with a new default value.
 */
public class V0_41_00_002__ChangeNotificationDefaultValue extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_41_00_002__ChangeNotificationDefaultValue.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());

    dropNotifySchemaChanges(ctx);
    addNotifySchemaChanges(ctx);
  }

  private static void dropNotifySchemaChanges(final DSLContext ctx) {
    ctx.alterTable("connection")
        .dropColumn(DSL.field(
            "notify_schema_changes"))
        .execute();
  }

  private static void addNotifySchemaChanges(final DSLContext ctx) {
    ctx.alterTable("connection")
        .addColumnIfNotExists(DSL.field(
            "notify_schema_changes",
            SQLDataType.BOOLEAN.nullable(false).defaultValue(false)))
        .execute();
  }

}
