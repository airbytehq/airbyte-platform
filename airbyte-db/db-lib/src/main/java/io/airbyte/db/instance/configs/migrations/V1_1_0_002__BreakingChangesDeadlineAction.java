/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V1_1_0_002__BreakingChangesDeadlineAction extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_0_002__BreakingChangesDeadlineAction.class);
  private static final Table<Record> BREAKING_CHANGE_TABLE = DSL.table("actor_definition_breaking_change");
  private static final Field<String> DEADLINE_ACTION = DSL.field("deadline_action", SQLDataType.VARCHAR(256));

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());
    ctx.alterTable(BREAKING_CHANGE_TABLE).add(DEADLINE_ACTION).execute();
  }

}
