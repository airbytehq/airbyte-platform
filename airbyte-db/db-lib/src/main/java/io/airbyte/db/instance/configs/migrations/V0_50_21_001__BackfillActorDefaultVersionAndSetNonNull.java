/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import com.google.common.annotations.VisibleForTesting;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sets all actor's default_version_id to its actor_definition's default_version_id, and sets the
 * column to be non-null.
 */
public class V0_50_21_001__BackfillActorDefaultVersionAndSetNonNull extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_21_001__BackfillActorDefaultVersionAndSetNonNull.class);

  private static final Table<Record> ACTOR_DEFINITION = DSL.table("actor_definition");
  private static final Table<Record> ACTOR = DSL.table("actor");

  private static final Field<UUID> ID = DSL.field("id", SQLDataType.UUID);
  private static final Field<UUID> DEFAULT_VERSION_ID = DSL.field("default_version_id", SQLDataType.UUID);
  private static final Field<UUID> ACTOR_DEFINITION_ID = DSL.field("actor_definition_id", SQLDataType.UUID);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    backfillActorDefaultVersionId(ctx);
    setNonNull(ctx);
  }

  @VisibleForTesting
  static void backfillActorDefaultVersionId(final DSLContext ctx) {
    final var actorDefinitions = ctx.select(ID, DEFAULT_VERSION_ID)
        .from(ACTOR_DEFINITION)
        .fetch();

    for (final var actorDefinition : actorDefinitions) {
      final UUID actorDefinitionId = actorDefinition.get(ID);
      final UUID defaultVersionId = actorDefinition.get(DEFAULT_VERSION_ID);

      ctx.update(ACTOR)
          .set(DEFAULT_VERSION_ID, defaultVersionId)
          .where(ACTOR_DEFINITION_ID.eq(actorDefinitionId))
          .execute();
    }
  }

  @VisibleForTesting
  static void setNonNull(final DSLContext ctx) {
    ctx.alterTable(ACTOR)
        .alterColumn(DEFAULT_VERSION_ID)
        .setNotNull()
        .execute();
  }

}
