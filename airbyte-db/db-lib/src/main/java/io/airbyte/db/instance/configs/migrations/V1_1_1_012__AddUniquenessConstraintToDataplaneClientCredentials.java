/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.unique;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Migration to add a uniqueness constraint on client_id in the dataplane_client_credentials table.
 */
public class V1_1_1_012__AddUniquenessConstraintToDataplaneClientCredentials extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_1_012__AddUniquenessConstraintToDataplaneClientCredentials.class);
  private static final String DATAPLANE_CLIENT_CREDENTIALS_TABLE = "dataplane_client_credentials";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migrations may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    addUniqueConstraintOnClientId(ctx);
  }

  private static void addUniqueConstraintOnClientId(final DSLContext ctx) {
    ctx.alterTable(DATAPLANE_CLIENT_CREDENTIALS_TABLE)
        .add(unique("client_id"))
        .execute();
  }

}
