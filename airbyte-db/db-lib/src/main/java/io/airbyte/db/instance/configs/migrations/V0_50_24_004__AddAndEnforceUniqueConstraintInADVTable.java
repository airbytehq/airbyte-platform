/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import com.google.common.annotations.VisibleForTesting;
import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Select;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adds a missed unique constraint on (actor definition id, version) pair in the actor definition
 * version table. Removes duplicate rows if any.
 */
public class V0_50_24_004__AddAndEnforceUniqueConstraintInADVTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_24_004__AddAndEnforceUniqueConstraintInADVTable.class);
  private static final Table<?> ACTOR_DEFINITION_VERSION = DSL.table("actor_definition_version");
  private static final Field<UUID> ID = DSL.field("id", UUID.class);
  private static final Field<Integer> ACTOR_DEFINITION_ID = DSL.field("actor_definition_id", Integer.class);
  private static final Field<String> DOCKER_IMAGE_TAG = DSL.field("docker_image_tag", String.class);
  private static final Field<Timestamp> CREATED_AT = DSL.field("created_at", Timestamp.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    migrate(ctx);
  }

  @VisibleForTesting
  static void migrate(final DSLContext ctx) {
    dropDuplicateRows(ctx);
    dropNonUniqueIndex(ctx);
    addUniqueConstraint(ctx);
  }

  private static void dropDuplicateRows(final DSLContext ctx) {
    ctx.transaction(configuration -> {
      final var transactionCtx = DSL.using(configuration);

      // Define the ranking logic within a select query
      final Select<?> rankingQuery = transactionCtx.select(ID,
          DSL.rowNumber()
              .over(DSL.partitionBy(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG)
                  .orderBy(CREATED_AT))
              .as("creation_rank"))
          .from(ACTOR_DEFINITION_VERSION);

      // Fetch IDs with creation_rank > 1 from the query
      final List<UUID> idsToDelete = transactionCtx.select(rankingQuery.field(ID))
          .from(rankingQuery)
          .where(rankingQuery.field("creation_rank", Integer.class).gt(1))
          .fetchInto(UUID.class);

      LOGGER.info("Deleting {} duplicate (on actor def id + docker image tag) rows from the ADV table.", idsToDelete.size());

      // Delete rows based on fetched IDs
      if (!idsToDelete.isEmpty()) {
        transactionCtx.deleteFrom(ACTOR_DEFINITION_VERSION)
            .where(ID.in(idsToDelete))
            .execute();
      }
    });

  }

  private static void dropNonUniqueIndex(final DSLContext ctx) {
    ctx.dropIndexIfExists("actor_definition_version_definition_image_tag_idx").on(ACTOR_DEFINITION_VERSION).execute();
  }

  private static void addUniqueConstraint(final DSLContext ctx) {
    ctx.alterTable(ACTOR_DEFINITION_VERSION)
        .add(DSL.constraint("actor_definition_version_actor_definition_id_version_key").unique("actor_definition_id", "docker_image_tag"))
        .execute();
  }

}
