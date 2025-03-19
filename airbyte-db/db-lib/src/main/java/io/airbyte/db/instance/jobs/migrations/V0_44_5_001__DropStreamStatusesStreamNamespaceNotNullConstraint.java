/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drops not null constraint on stream_statuses#stream_namespace column.
 */
public class V0_44_5_001__DropStreamStatusesStreamNamespaceNotNullConstraint extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_44_5_001__DropStreamStatusesStreamNamespaceNotNullConstraint.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());
    ctx.alterTable("stream_statuses").alter("stream_namespace").dropNotNull().execute();

    LOGGER.info("Completed migration: {}", this.getClass().getSimpleName());
  }

}
