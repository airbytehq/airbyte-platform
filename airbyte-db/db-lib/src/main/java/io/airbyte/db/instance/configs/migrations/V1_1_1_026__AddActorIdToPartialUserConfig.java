/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import com.google.common.annotations.VisibleForTesting;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V1_1_1_026__AddActorIdToPartialUserConfig extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_1_026__AddActorIdToPartialUserConfig.class);
  static final String PARTIAL_USER_CONFIG_TABLE_NAME = "partial_user_config";
  static final String PARTIAL_USER_CONFIG_ACTOR_ID_FK = "partial_user_config_actor_id_fkey";
  static final String ID_FIELD = "id";
  static final String ACTOR_TABLE = "actor";
  static final String ACTOR_ID_FIELD = "actor_id";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    addActorIdFieldAndForeignKeyConstraint(ctx);
  }

  @VisibleForTesting
  public static void addActorIdFieldAndForeignKeyConstraint(final DSLContext ctx) {
    ctx.alterTable(PARTIAL_USER_CONFIG_TABLE_NAME)
        .addColumnIfNotExists(DSL.field(ACTOR_ID_FIELD, SQLDataType.UUID))
        .execute();

    ctx.alterTable(PARTIAL_USER_CONFIG_TABLE_NAME)
        .add(DSL.constraint(PARTIAL_USER_CONFIG_ACTOR_ID_FK)
            .foreignKey(ACTOR_ID_FIELD)
            .references(ACTOR_TABLE, ID_FIELD)
            .onDeleteCascade())
        .execute();
  }

}
