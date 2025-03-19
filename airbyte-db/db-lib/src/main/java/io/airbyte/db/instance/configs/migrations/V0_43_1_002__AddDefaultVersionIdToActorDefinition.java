/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.constraint;

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
 * Add default_version_id to actor_definition table, referencing actor_definition_version table.
 */
public class V0_43_1_002__AddDefaultVersionIdToActorDefinition extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_43_1_002__AddDefaultVersionIdToActorDefinition.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());
    addDefaultVersionIdToActorDefinition(ctx);
  }

  private static void addDefaultVersionIdToActorDefinition(final DSLContext ctx) {
    // This is nullable so that if we need to create a new actor_definition, we can do so before
    // subsequently creating the associated actor_definition_version and filling in the
    // default_version_id
    // field pointing to the new actor_definition_version.
    final Field<UUID> defaultVersionId = DSL.field("default_version_id", SQLDataType.UUID.nullable(true));

    ctx.alterTable("actor_definition")
        .addIfNotExists(defaultVersionId).execute();
    ctx.alterTable("actor_definition")
        .add(constraint("actor_definition_default_version_id_fkey").foreignKey(defaultVersionId)
            .references("actor_definition_version", "id").onDeleteRestrict())
        .execute();
  }

}
