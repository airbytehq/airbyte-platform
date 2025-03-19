/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This migration fixes an inconsistency in specs for declarative sources. We were not updating the
 * spec in the actor_definition_version table when setting a new active declarative manifest
 * version, so the spec in the actor_definition_version table could be different from the spec in
 * the actor_definition table.
 *
 * This migration updates the spec in the actor_definition_version table to match the spec in the
 * actor_definition table to make sure things are consistent before we eventually fully remove the
 * field from the actor_definition table.
 */
public class V0_50_4_001__FixInconsistentDeclarativeSpecs extends BaseJavaMigration {

  private static final Table<Record> ACTOR_DEFINITION_VERSION = DSL.table("actor_definition_version");
  private static final Table<Record> ACTOR_DEFINITION = DSL.table("actor_definition");

  private static final Field<UUID> DEFAULT_VERSION_ID = DSL.field(DSL.name("actor_definition", "default_version_id"), SQLDataType.UUID);
  private static final Field<UUID> ACTOR_DEFINITION_VERSION_ID = DSL.field(DSL.name("actor_definition_version", "id"), SQLDataType.UUID);
  private static final Field<JSONB> ACTOR_DEFINITION_SPEC = DSL.field(DSL.name("actor_definition", "spec"), SQLDataType.JSONB);
  private static final Field<JSONB> ACTOR_DEFINITION_VERSION_SPEC = DSL.field(DSL.name("actor_definition_version", "spec"), SQLDataType.JSONB);

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_4_001__FixInconsistentDeclarativeSpecs.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    updateMismatchedSpecs(ctx);
  }

  private void updateMismatchedSpecs(final DSLContext ctx) {
    // Look up all actor definitions whose spec is different from its default version's spec
    final var mismatchedSpecs = ctx.select(ACTOR_DEFINITION_VERSION_ID, ACTOR_DEFINITION_SPEC)
        .from(ACTOR_DEFINITION)
        .join(ACTOR_DEFINITION_VERSION).on(ACTOR_DEFINITION_VERSION_ID.eq(DEFAULT_VERSION_ID))
        .where(ACTOR_DEFINITION_VERSION_SPEC.ne(ACTOR_DEFINITION_SPEC))
        .fetch();

    // update actor_definition_version records with the spec from the actor_definition table
    mismatchedSpecs.forEach(record -> {
      final UUID actorDefVersionId = record.get(ACTOR_DEFINITION_VERSION_ID);
      final JSONB newSpec = record.get(ACTOR_DEFINITION_SPEC);
      ctx.update(ACTOR_DEFINITION_VERSION)
          .set(ACTOR_DEFINITION_VERSION_SPEC, newSpec)
          .where(ACTOR_DEFINITION_VERSION_ID.eq(actorDefVersionId))
          .execute();
    });
  }

}
