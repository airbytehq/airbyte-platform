/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.foreignKey;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_57_4_014__AddOnDeleteTimelineUserId extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_57_4_014__AddOnDeleteTimelineUserId.class);

  private static final Table<Record> CONNECTION_TIMELINE_EVENT = DSL.table("connection_timeline_event");

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());

    ctx.alterTable(CONNECTION_TIMELINE_EVENT).dropConstraint("connection_timeline_event_user_id_fkey").execute();
    ctx.alterTable("connection_timeline_event").add(foreignKey("user_id")
        .references("user", "id")
        .onDeleteSetNull()).execute();
  }

}
