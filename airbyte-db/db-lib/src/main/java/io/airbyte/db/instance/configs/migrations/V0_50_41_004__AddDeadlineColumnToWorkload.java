/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import java.time.OffsetDateTime;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_50_41_004__AddDeadlineColumnToWorkload extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_41_004__AddDeadlineColumnToWorkload.class);
  private static final String WORKLOAD_TABLE = "workload";
  private static final String DEADLINE_COLUMN = "deadline";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    addDeadlineColumnToWorkload(ctx);
  }

  public static void addDeadlineColumnToWorkload(final DSLContext ctx) {
    final Field<OffsetDateTime> createdAt =
        DSL.field(DEADLINE_COLUMN, SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(true));
    ctx.alterTable("workload")
        .addColumnIfNotExists(createdAt)
        .execute();
    ctx.execute(String.format("CREATE INDEX ON %s(%s) WHERE %s IS NOT NULL", WORKLOAD_TABLE, DEADLINE_COLUMN, DEADLINE_COLUMN));
  }

}
