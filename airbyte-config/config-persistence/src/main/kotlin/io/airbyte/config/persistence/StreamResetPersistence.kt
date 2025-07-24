/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.config.StreamDescriptor
import io.airbyte.config.StreamResetRecord
import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.instance.configs.jooq.generated.Tables
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.RecordMapper
import org.jooq.impl.DSL
import java.io.IOException
import java.time.OffsetDateTime
import java.util.UUID
import java.util.stream.Stream

/**
 * Persistence that contains which streams are marked as needing a reset for a connection.
 */
class StreamResetPersistence(
  database: Database?,
) {
  private val database = ExceptionWrappingDatabase(database)

  /**
   * Get a list of StreamDescriptors for streams that have pending or running resets.
   *
   * @param connectionId connection id
   * @return streams marked as needed resets
   * @throws IOException if there is an issue while interacting with the db.
   */
  @Throws(IOException::class)
  fun getStreamResets(connectionId: UUID?): List<StreamDescriptor> =
    database
      .query { ctx: DSLContext ->
        ctx
          .select(DSL.asterisk())
          .from(Tables.STREAM_RESET)
      }.where(Tables.STREAM_RESET.CONNECTION_ID.eq(connectionId))
      .fetch(getStreamResetRecordMapper())
      .stream()
      .flatMap { row: StreamResetRecord -> Stream.of(StreamDescriptor().withName(row.streamName).withNamespace(row.streamNamespace)) }
      .toList()

  /**
   * Delete stream resets for a given connection. This is called to delete stream reset records for
   * resets that are successfully completed.
   *
   * @param connectionId connection id
   * @param streamsToDelete streams to delete
   * @throws IOException if there is an issue while interacting with the db.
   */
  @Throws(IOException::class)
  fun deleteStreamResets(
    connectionId: UUID?,
    streamsToDelete: List<StreamDescriptor>,
  ) {
    var condition = DSL.noCondition()
    for (streamDescriptor in streamsToDelete) {
      condition =
        condition.or(
          Tables.STREAM_RESET.CONNECTION_ID
            .eq(connectionId)
            .and(Tables.STREAM_RESET.STREAM_NAME.eq(streamDescriptor.name))
            .and(PersistenceHelpers.isNullOrEquals(Tables.STREAM_RESET.STREAM_NAMESPACE, streamDescriptor.namespace)),
        )
    }

    database
      .query { ctx: DSLContext ->
        ctx.deleteFrom(
          Tables.STREAM_RESET,
        )
      }.where(condition)
      .execute()
  }

  /**
   * Create stream resets for a given connection. This is called to create stream reset records for
   * resets that are going to be run.
   *
   * It will not attempt to create entries for any stream that already exists in the stream_reset
   * table.
   */
  @Throws(IOException::class)
  fun createStreamResets(
    connectionId: UUID,
    streamsToCreate: List<StreamDescriptor>,
  ) {
    val timestamp = OffsetDateTime.now()
    database.transaction<Any?> { ctx: DSLContext ->
      createStreamResets(ctx, connectionId, streamsToCreate, timestamp)
      null
    }
  }

  private fun createStreamResets(
    ctx: DSLContext,
    connectionId: UUID,
    streamsToCreate: List<StreamDescriptor>,
    timestamp: OffsetDateTime,
  ) {
    for (streamDescriptor in streamsToCreate) {
      val streamExists =
        ctx.fetchExists(
          Tables.STREAM_RESET,
          Tables.STREAM_RESET.CONNECTION_ID.eq(connectionId),
          Tables.STREAM_RESET.STREAM_NAME.eq(streamDescriptor.name),
          PersistenceHelpers.isNullOrEquals(Tables.STREAM_RESET.STREAM_NAMESPACE, streamDescriptor.namespace),
        )

      if (!streamExists) {
        ctx
          .insertInto(Tables.STREAM_RESET)
          .set(Tables.STREAM_RESET.ID, UUID.randomUUID())
          .set(Tables.STREAM_RESET.CONNECTION_ID, connectionId)
          .set(Tables.STREAM_RESET.STREAM_NAME, streamDescriptor.name)
          .set(Tables.STREAM_RESET.STREAM_NAMESPACE, streamDescriptor.namespace)
          .set(Tables.STREAM_RESET.CREATED_AT, timestamp)
          .set(Tables.STREAM_RESET.UPDATED_AT, timestamp)
          .execute()
      }
    }
  }

  companion object {
    private fun getStreamResetRecordMapper(): RecordMapper<Record, StreamResetRecord> =
      RecordMapper { record: Record ->
        StreamResetRecord(
          UUID.fromString(
            record.get(
              Tables.STREAM_RESET.CONNECTION_ID,
              String::class.java,
            ),
          ),
          record.get(Tables.STREAM_RESET.STREAM_NAME, String::class.java),
          record.get(Tables.STREAM_RESET.STREAM_NAMESPACE, String::class.java),
        )
      }
  }
}
