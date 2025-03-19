/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations;

import static org.jooq.impl.DSL.foreignKey;
import static org.jooq.impl.DSL.primaryKey;

import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_57_2_004__AddStreamAttemptMetadata extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_57_2_004__AddStreamAttemptMetadata.class);
  private static final String STREAM_ATTEMPT_METADATA_TABLE_NAME = "stream_attempt_metadata";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());

    final Field<UUID> id = DSL.field("id", SQLDataType.UUID.notNull());
    final Field<Integer> attemptId = DSL.field("attempt_id", SQLDataType.INTEGER.nullable(false));
    final Field<String> streamNamespace = DSL.field("stream_namespace", SQLDataType.VARCHAR);
    final Field<String> streamName = DSL.field("stream_name", SQLDataType.VARCHAR.notNull());
    final Field<Boolean> wasBackfilled = DSL.field("was_backfilled", SQLDataType.BOOLEAN.nullable(false).defaultValue(false));
    final Field<Boolean> wasResumed = DSL.field("was_resumed", SQLDataType.BOOLEAN.nullable(false).defaultValue(false));

    ctx.createTableIfNotExists(STREAM_ATTEMPT_METADATA_TABLE_NAME)
        .columns(id, attemptId, streamNamespace, streamName, wasBackfilled, wasResumed)
        .constraints(
            primaryKey(id),
            foreignKey(attemptId).references("attempts", "id").onDeleteCascade())
        .execute();

    // We expect attemptId based look ups
    ctx.createIndexIfNotExists("stream_attempt_metadata__attempt_id_idx")
        .on(STREAM_ATTEMPT_METADATA_TABLE_NAME, attemptId.getName())
        .execute();

    // Uniqueness constraint on name, namespace per attempt to avoid duplicates
    ctx.createUniqueIndexIfNotExists("stream_attempt_metadata__attempt_id_name_namespace_idx")
        .on(STREAM_ATTEMPT_METADATA_TABLE_NAME, attemptId.getName(), streamNamespace.getName(), streamName.getName())
        .where(streamNamespace.isNotNull())
        .execute();

    // Workaround for namespace being null and pg dropping null values from indexes
    ctx.createUniqueIndexIfNotExists("stream_attempt_metadata__attempt_id_name_idx")
        .on(STREAM_ATTEMPT_METADATA_TABLE_NAME, attemptId.getName(), streamName.getName())
        .where(streamNamespace.isNull())
        .execute();
  }

}
