/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V1_1_1_003__AddConnectionTagIndex extends BaseJavaMigration {

  private static final String CONNECTION_TAG_TABLE = "connection_tag";

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_1_003__AddConnectionTagIndex.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());

    migrate(ctx);
  }

  public static void migrate(final DSLContext ctx) {
    ctx.createIndexIfNotExists("connection_tag_connection_id_idx")
        .on(CONNECTION_TAG_TABLE, "connection_id")
        .execute();
  }

}
