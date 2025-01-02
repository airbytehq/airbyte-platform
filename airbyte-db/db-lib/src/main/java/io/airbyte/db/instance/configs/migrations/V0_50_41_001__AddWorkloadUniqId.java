/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_50_41_001__AddWorkloadUniqId extends BaseJavaMigration {

  private static final String TABLE = "workload";
  private static final String AUTO_ID = "auto_id";

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_41_001__AddWorkloadUniqId.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());

    final Field<UUID> autoIdField = DSL.field(AUTO_ID, SQLDataType.UUID.nullable(true));
    ctx.alterTable(TABLE)
        .addColumnIfNotExists(autoIdField)
        .execute();
  }

}
