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

public class V0_50_4_002__AddScopeStatusCreatedAtIndex extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_4_002__AddScopeStatusCreatedAtIndex.class);

  static final String SCOPE_STATUS_CREATED_AT_INDEX_NAME = "scope_status_created_at_idx";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());
    ctx.query("CREATE INDEX CONCURRENTLY IF NOT EXISTS " + SCOPE_STATUS_CREATED_AT_INDEX_NAME + " ON jobs(scope, status, created_at DESC)").execute();
  }

  // This prevents flyway from automatically wrapping the migration in a transaction.
  // This is important because indexes cannot be created concurrently (i.e. without locking) from
  // within a transaction.
  @Override
  public boolean canExecuteInTransaction() {
    return false;
  }

}
