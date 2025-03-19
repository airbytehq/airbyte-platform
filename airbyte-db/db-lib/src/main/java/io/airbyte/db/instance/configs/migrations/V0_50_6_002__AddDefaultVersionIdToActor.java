/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.foreignKey;

import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Inserts an default_version_id column to the actor table. The default_version_id is a foreign key
 * to the id of the actor_definition_version table.
 */
public class V0_50_6_002__AddDefaultVersionIdToActor extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_6_002__AddDefaultVersionIdToActor.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    addDefaultVersionIdColumnToActor(ctx);
  }

  static void addDefaultVersionIdColumnToActor(final DSLContext ctx) {
    // TODO (connector-ops) Remove nullable
    final Field<UUID> defaultVersionId = DSL.field("default_version_id", SQLDataType.UUID.nullable(true));

    ctx.alterTable("actor").addColumnIfNotExists(defaultVersionId).execute();
    ctx.alterTable("actor").add(foreignKey(defaultVersionId).references("actor_definition_version")).execute();

    LOGGER.info("default_version_id column added to actor table");
  }

}
