/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations;

import static io.airbyte.db.instance.jobs.migrations.V0_50_4_002__AddScopeStatusCreatedAtIndex.SCOPE_STATUS_CREATED_AT_INDEX_NAME;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_50_4_003__AddScopeCreatedAtScopeNonTerminalIndexes extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_4_003__AddScopeCreatedAtScopeNonTerminalIndexes.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());
    // helps with the general sorting of jobs by latest per connection
    ctx.query("CREATE INDEX CONCURRENTLY IF NOT EXISTS scope_created_at_idx ON jobs(scope, created_at DESC)").execute();

    // helps for looking for active jobs
    ctx.query(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS scope_non_terminal_status_idx ON jobs(scope, status) "
            + "WHERE status NOT IN ('failed', 'succeeded', 'cancelled')")
        .execute();

    // remove other index, as these two are more performant
    ctx.query("DROP INDEX CONCURRENTLY " + SCOPE_STATUS_CREATED_AT_INDEX_NAME).execute();
  }

  // This prevents flyway from automatically wrapping the migration in a transaction.
  // This is important because indexes cannot be created concurrently (i.e. without locking) from
  // within a transaction.
  @Override
  public boolean canExecuteInTransaction() {
    return false;
  }

}
