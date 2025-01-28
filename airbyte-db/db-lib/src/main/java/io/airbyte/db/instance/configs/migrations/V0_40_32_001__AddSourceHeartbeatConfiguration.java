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
 * Add override for what the heartbeat should be for a specific connector definition.
 */
public class V0_40_32_001__AddSourceHeartbeatConfiguration extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_40_32_001__AddSourceHeartbeatConfiguration.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());
    addSourceHeartbeatConfiguration(ctx);
  }

  private static void addSourceHeartbeatConfiguration(final DSLContext ctx) {
    ctx.alterTable("actor_definition")
        .addColumnIfNotExists(DSL.field(
            "max_seconds_between_messages",
            SQLDataType.INTEGER.nullable(true)))
        .execute();

    ctx.commentOnColumn(DSL.name("actor_definition", "max_seconds_between_messages"))
        .is("Define the number of seconds allowed between 2 messages emitted by the connector before timing out")
        .execute();
  }

}
