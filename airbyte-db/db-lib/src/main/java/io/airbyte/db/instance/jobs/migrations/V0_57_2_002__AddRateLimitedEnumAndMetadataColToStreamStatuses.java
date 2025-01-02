/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations;

import com.google.common.annotations.VisibleForTesting;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_57_2_002__AddRateLimitedEnumAndMetadataColToStreamStatuses extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_57_2_002__AddRateLimitedEnumAndMetadataColToStreamStatuses.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    migrate(ctx);
  }

  @VisibleForTesting
  public static void migrate(final DSLContext ctx) {
    ctx.alterTable("stream_statuses")
        .addColumnIfNotExists(DSL.field("metadata", SQLDataType.JSONB.nullable(true)))
        .execute();

    ctx.alterType("job_stream_status_run_state").addValue("rate_limited").execute();
  }

}
