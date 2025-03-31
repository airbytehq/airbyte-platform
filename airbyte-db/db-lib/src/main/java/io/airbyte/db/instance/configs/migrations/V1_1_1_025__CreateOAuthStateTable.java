/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.jooq.impl.DSL.primaryKey;

import java.time.OffsetDateTime;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V1_1_1_025__CreateOAuthStateTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_1_025__CreateOAuthStateTable.class);

  static final Field<String> id = DSL.field("id", SQLDataType.VARCHAR.notNull());
  static final Field<String> state = DSL.field("state", SQLDataType.VARCHAR.notNull());
  // row timestamps
  static final Field<OffsetDateTime> createdAt = DSL
      .field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(currentOffsetDateTime()));
  static final Field<OffsetDateTime> updatedAt = DSL
      .field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(currentOffsetDateTime()));

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    createOAuthStateTable(ctx);
  }

  private static void createOAuthStateTable(final DSLContext ctx) {
    ctx.createTableIfNotExists("oauth_state")
        .columns(id, state, createdAt, updatedAt)
        .constraints(primaryKey(id))
        .execute();
  }

}
