/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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

/**
 * Adds notification_settings column to workspace table. This intends to replace notification column
 * in the near future.
 */
public class V0_44_5_001__AddNotificationSettingsColumnToWorkspace extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_44_5_001__AddNotificationSettingsColumnToWorkspace.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());
    addNotificationSettingsColumnToWorkspace(ctx);
  }

  private static void addNotificationSettingsColumnToWorkspace(final DSLContext ctx) {
    final Field<JSONB> notificationSettings = DSL.field("notification_settings", SQLDataType.JSONB.nullable(true));

    ctx.alterTable("workspace")
        .addIfNotExists(notificationSettings).execute();
  }

}
