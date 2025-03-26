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

public class V0_57_4_015__AddLanguageToActorDefinitionVersion extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_57_4_015__AddLanguageToActorDefinitionVersion.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());
    final DSLContext ctx = DSL.using(context.getConnection());
    addLanguageColumnToActorDefinitionVersion(ctx);
    LOGGER.info("language column added to actor_definition_version table");
  }

  static void addLanguageColumnToActorDefinitionVersion(final DSLContext ctx) {
    ctx.alterTable("actor_definition_version")
        .addColumnIfNotExists(DSL.field("language", SQLDataType.VARCHAR(256).nullable(true)))
        .execute();
  }

}
