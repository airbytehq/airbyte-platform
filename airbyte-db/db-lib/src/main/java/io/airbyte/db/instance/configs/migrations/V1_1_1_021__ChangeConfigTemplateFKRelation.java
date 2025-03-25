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

public class V1_1_1_021__ChangeConfigTemplateFKRelation extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_1_021__ChangeConfigTemplateFKRelation.class);

  static final String CONFIG_TEMPLATE_TABLE_NAME = "config_template";
  static final String CONFIG_TEMPLATE_ACTOR_FK = "config_template_actor_definition_id_fkey";
  static final String ACTOR_DEFINITION_ID_FIELD = "actor_definition_id";
  static final String ACTOR_DEFINITION_TABLE = "actor_definition";
  static final String ID_FIELD = "id";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());
    final DSLContext ctx = DSL.using(context.getConnection());
    doMigration(ctx);
  }

  static void doMigration(final DSLContext ctx) {
    ctx.alterTable(CONFIG_TEMPLATE_TABLE_NAME)
        .dropIfExists(DSL.constraint(CONFIG_TEMPLATE_ACTOR_FK))
        .execute();

    ctx.alterTable(CONFIG_TEMPLATE_TABLE_NAME)
        .add(DSL.constraint(CONFIG_TEMPLATE_ACTOR_FK)
            .foreignKey(ACTOR_DEFINITION_ID_FIELD)
            .references(ACTOR_DEFINITION_TABLE, ID_FIELD)
            .onDeleteCascade())
        .execute();

  }

}
