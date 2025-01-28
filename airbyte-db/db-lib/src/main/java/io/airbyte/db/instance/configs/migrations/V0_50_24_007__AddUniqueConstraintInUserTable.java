/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.db.instance.configs.migrations.V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.AuthProvider;
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
 * Adds a missed unique constraint on (auth_user_id, auth_provider) pair in User table.
 */
public class V0_50_24_007__AddUniqueConstraintInUserTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_24_007__AddUniqueConstraintInUserTable.class);
  static final Table<?> USER_TABLE = DSL.table("\"user\""); // Using quotes in case it conflicts with the reserved "user" keyword in Postgres.
  private static final Field<UUID> ID = DSL.field("id", UUID.class);
  private static final Field<String> AUTH_USER_ID_FIELD = DSL.field("auth_user_id", String.class);
  private static final Field<AuthProvider> AUTH_PROVIDER_FIELD = DSL.field("auth_provider", AuthProvider.class);
  private static final Field<Timestamp> CREATED_AT = DSL.field("created_at", Timestamp.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());
    final DSLContext ctx = DSL.using(context.getConnection());
    migrate(ctx);
    LOGGER.info("Migration finished!");
  }

  @VisibleForTesting
  static void migrate(final DSLContext ctx) {
    dropDuplicateRows(ctx);
    addUniqueConstraint(ctx);
  }

  private static void dropDuplicateRows(final DSLContext ctx) {
    ctx.transaction(configuration -> {
      final var transactionCtx = DSL.using(configuration);

      // Define the ranking logic within a select query
      final Select<?> rankingQuery = transactionCtx.select(ID,
          DSL.rowNumber()
              .over(DSL.partitionBy(AUTH_USER_ID_FIELD, AUTH_PROVIDER_FIELD)
                  .orderBy(CREATED_AT))
              .as("creation_rank"))
          .from(USER_TABLE);

      // Fetch IDs with creation_rank > 1 from the query
      final List<UUID> userIdsToDelete = transactionCtx.select(rankingQuery.field(ID))
          .from(rankingQuery)
          .where(rankingQuery.field("creation_rank", Integer.class).gt(1))
          .fetchInto(UUID.class);

      LOGGER.info("Deleting {} duplicate (auth_user_id, auth_provider) rows from the User table.", userIdsToDelete.size());

      // Delete rows based on fetched IDs
      if (!userIdsToDelete.isEmpty()) {
        transactionCtx.deleteFrom(USER_TABLE)
            .where(ID.in(userIdsToDelete))
            .execute();
      }
    });

  }

  private static void addUniqueConstraint(final DSLContext ctx) {
    ctx.alterTable(USER_TABLE)
        .add(DSL.constraint("auth_user_id_auth_provider_key").unique(AUTH_USER_ID_FIELD, AUTH_PROVIDER_FIELD))
        .execute();
  }

}
