/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Inserts a support_level column to the actor_definition_version table.
 */
public class V0_57_4_007__AddInternalSupportLevelToActorDefinitionVersion extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_57_4_007__AddInternalSupportLevelToActorDefinitionVersion.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());
    final DSLContext ctx = DSL.using(context.getConnection());
    addInternalSupportLevelToActorDefinitionVersion(ctx);
    LOGGER.info("internal_support_level column added to actor_definition_version table");
  }

  static void addInternalSupportLevelToActorDefinitionVersion(final DSLContext ctx) {
    addInternalSupportLevelColumn(ctx);
  }

  static void addInternalSupportLevelColumn(final DSLContext ctx) {
    ctx.alterTable("actor_definition_version")
        .addColumnIfNotExists(DSL.field("internal_support_level", SQLDataType.BIGINT.nullable(false).defaultValue(100L)))
        .execute();
  }

}
