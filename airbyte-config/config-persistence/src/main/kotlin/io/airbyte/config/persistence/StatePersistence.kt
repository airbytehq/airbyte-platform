/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.json.Jsons
import io.airbyte.config.State
import io.airbyte.config.StateWrapper
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.helpers.ProtocolConverters.Companion.toInternal
import io.airbyte.config.helpers.StateMessageHelper.isMigration
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl
import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.StateType
import io.airbyte.protocol.models.v0.AirbyteGlobalState
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType
import io.airbyte.protocol.models.v0.AirbyteStreamState
import org.jooq.Condition
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.Record
import org.jooq.RecordMapper
import org.jooq.impl.DSL
import java.io.IOException
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID
import java.util.stream.Collectors

/**
 * State Persistence.
 *
 * Handle persisting States to the Database.
 *
 * Supports migration from Legacy to Global or Stream. Other type migrations need to go through a
 * reset. (an exception will be thrown)
 */
class StatePersistence(
  database: Database?,
  private val connectionServiceJooqImpl: ConnectionServiceJooqImpl,
) {
  private val database = ExceptionWrappingDatabase(database)

  /**
   * Get the current State of a Connection.
   *
   * @param connectionId connection id
   * @return current state for the connection
   * @throws IOException if there is an issue while interacting with the db.
   */
  @Throws(IOException::class)
  fun getCurrentState(connectionId: UUID): Optional<StateWrapper> {
    val records =
      database.query { ctx: DSLContext ->
        getStateRecords(
          ctx,
          connectionId,
        )
      }

    if (records.isEmpty()) {
      return Optional.empty()
    }

    return when (getStateType(connectionId, records)) {
      StateType.GLOBAL ->
        Optional.of(
          buildGlobalState(records),
        )

      StateType.STREAM -> Optional.of(buildStreamState(records))
      else -> Optional.of(buildLegacyState(records))
    }
  }

  /**
   * Create or update the states described in the StateWrapper. Null states will be deleted.
   *
   * The only state migrations supported are going from a Legacy state to either a Global or Stream
   * state. Other state type migrations should go through an explicit reset. An exception will be
   * thrown to prevent the system from getting into a bad state.
   *
   * @param connectionId connection id
   * @param state new state
   * @throws IOException if there is an issue while interacting with the db.
   */
  @Throws(IOException::class)
  fun updateOrCreateState(
    connectionId: UUID,
    state: StateWrapper,
  ) {
    val previousState = getCurrentState(connectionId)
    val currentStateType = state.stateType
    val isMigration = isMigration(currentStateType, previousState)

    // The only case where we allow a state migration is moving from LEGACY.
    // We expect any other migration to go through an explicit reset.
    check(!(!isMigration && previousState.isPresent && previousState.get().stateType != currentStateType)) {
      (
        "Unexpected type migration from '" + previousState.get().stateType + "' to '" + currentStateType +
          "'. Migration of StateType need to go through an explicit reset."
      )
    }

    database.transaction<Any?> { ctx: DSLContext ->
      if (isMigration) {
        clearLegacyState(ctx, connectionId)
      }
      when (state.stateType) {
        io.airbyte.config.StateType.GLOBAL ->
          saveGlobalState(
            ctx,
            connectionId,
            state.global.global,
          )

        io.airbyte.config.StateType.STREAM ->
          saveStreamState(
            ctx,
            connectionId,
            state.stateMessages,
          )

        io.airbyte.config.StateType.LEGACY ->
          saveLegacyState(
            ctx,
            connectionId,
            state.legacyState,
          )

        else -> {}
      }
      null
    }
  }

  /**
   * Remove all states entry for a connection.
   *
   * @param connectionId the id of the connection
   * @throws IOException if there is an issue while interacting with the db.
   */
  @Throws(IOException::class)
  fun eraseState(connectionId: UUID) {
    database.transaction<Any?> { ctx: DSLContext ->
      deleteStateRecords(ctx, connectionId)
      null
    }
  }

  @Throws(IOException::class)
  private fun getAllStreamsForConnection(connectionId: UUID): Set<StreamDescriptor> =
    try {
      HashSet(connectionServiceJooqImpl.getAllStreamsForConnection(connectionId))
    } catch (e: ConfigNotFoundException) {
      emptySet()
    }

  @Throws(IOException::class)
  fun bulkDelete(
    connectionId: UUID,
    streamsToDelete: Set<StreamDescriptor>?,
  ) {
    if (streamsToDelete == null || streamsToDelete.isEmpty()) {
      return
    }

    val maybeCurrentState = getCurrentState(connectionId)
    if (maybeCurrentState.isEmpty) {
      return
    }

    val streamsInState =
      if (maybeCurrentState.get().stateType == io.airbyte.config.StateType.GLOBAL) {
        maybeCurrentState
          .get()
          .global.global.streamStates
          .stream()
          .map { obj: AirbyteStreamState -> obj.streamDescriptor }
          .map { obj: io.airbyte.protocol.models.v0.StreamDescriptor -> obj.toInternal() }
          .collect(Collectors.toSet())
      } else {
        maybeCurrentState
          .get()
          .stateMessages
          .stream()
          .map { airbyteStateMessage: AirbyteStateMessage -> airbyteStateMessage.stream.streamDescriptor }
          .map { obj: io.airbyte.protocol.models.v0.StreamDescriptor -> obj.toInternal() }
          .collect(Collectors.toSet())
      }

    if (streamsInState == streamsToDelete) {
      eraseState(connectionId)
    } else {
      val allStreamsForConnection = getAllStreamsForConnection(connectionId)
      if (allStreamsForConnection.isEmpty() || allStreamsForConnection == streamsToDelete) {
        eraseState(connectionId)
      } else {
        val conditions =
          streamsToDelete
            .stream()
            .map { stream: StreamDescriptor ->
              val nameCondition = DSL.field(DSL.name(Tables.STATE.STREAM_NAME.name)).eq(stream.name)
              val connCondition = DSL.field(DSL.name(Tables.STATE.CONNECTION_ID.name)).eq(connectionId)
              val namespaceCondition =
                if (stream.namespace == null) {
                  DSL.field(DSL.name(Tables.STATE.NAMESPACE.name)).isNull()
                } else {
                  DSL.field(DSL.name(Tables.STATE.NAMESPACE.name)).eq(stream.namespace)
                }
              DSL.and(namespaceCondition, nameCondition, connCondition)
            }.reduce(DSL.noCondition()) { left: Condition?, right: Condition? -> DSL.or(left, right) }
        database.transaction { ctx: DSLContext ->
          ctx
            .deleteFrom(
              Tables.STATE,
            ).where(conditions)
            .execute()
        }
      }
    }
  }

  @JvmRecord
  private data class StateRecord(
    val type: StateType,
    val streamName: String?,
    val namespace: String?,
    val state: JsonNode,
  )

  companion object {
    private fun clearLegacyState(
      ctx: DSLContext,
      connectionId: UUID,
    ) {
      val stateUpdateBatch = StateUpdateBatch()
      writeStateToDb(ctx, connectionId, null, null, io.airbyte.config.StateType.LEGACY, null, stateUpdateBatch)
      stateUpdateBatch.save(ctx)
    }

    private fun saveGlobalState(
      ctx: DSLContext,
      connectionId: UUID,
      globalState: AirbyteGlobalState,
    ) {
      val stateUpdateBatch = StateUpdateBatch()
      writeStateToDb(ctx, connectionId, null, null, io.airbyte.config.StateType.GLOBAL, globalState.sharedState, stateUpdateBatch)
      for (streamState in globalState.streamStates) {
        writeStateToDb(
          ctx,
          connectionId,
          streamState.streamDescriptor.name,
          streamState.streamDescriptor.namespace,
          io.airbyte.config.StateType.GLOBAL,
          streamState.streamState,
          stateUpdateBatch,
        )
      }
      stateUpdateBatch.save(ctx)
    }

    private fun saveStreamState(
      ctx: DSLContext,
      connectionId: UUID,
      stateMessages: List<AirbyteStateMessage>,
    ) {
      val stateUpdateBatch = StateUpdateBatch()
      for (stateMessage in stateMessages) {
        val streamState = stateMessage.stream
        writeStateToDb(
          ctx,
          connectionId,
          streamState.streamDescriptor.name,
          streamState.streamDescriptor.namespace,
          io.airbyte.config.StateType.STREAM,
          streamState.streamState,
          stateUpdateBatch,
        )
      }
      stateUpdateBatch.save(ctx)
    }

    private fun saveLegacyState(
      ctx: DSLContext,
      connectionId: UUID,
      state: JsonNode?,
    ) {
      val stateUpdateBatch = StateUpdateBatch()
      writeStateToDb(ctx, connectionId, null, null, io.airbyte.config.StateType.LEGACY, state, stateUpdateBatch)
      stateUpdateBatch.save(ctx)
    }

    /**
     * Performs the actual SQL operation depending on the state.
     *
     * If the state is null, it will delete the row, otherwise do an insert or update on conflict
     */
    fun writeStateToDb(
      ctx: DSLContext,
      connectionId: UUID,
      streamName: String?,
      namespace: String?,
      stateType: io.airbyte.config.StateType,
      state: JsonNode?,
      stateUpdateBatch: StateUpdateBatch,
    ) {
      if (state != null) {
        val hasState =
          ctx.fetchExists(
            Tables.STATE,
            Tables.STATE.CONNECTION_ID.eq(connectionId),
            PersistenceHelpers.isNullOrEquals(Tables.STATE.STREAM_NAME, streamName),
            PersistenceHelpers.isNullOrEquals(Tables.STATE.NAMESPACE, namespace),
          )

        // NOTE: the legacy code was storing a State object instead of just the State data field. We kept
        // the same behavior for consistency.
        val jsonbState =
          JSONB.valueOf(Jsons.serialize(if (stateType != io.airbyte.config.StateType.LEGACY) state else State().withState(state)))
        val now = OffsetDateTime.now()

        if (!hasState) {
          stateUpdateBatch.createdStreamStates.add(
            ctx
              .insertInto(Tables.STATE)
              .columns(
                Tables.STATE.ID,
                Tables.STATE.CREATED_AT,
                Tables.STATE.UPDATED_AT,
                Tables.STATE.CONNECTION_ID,
                Tables.STATE.STREAM_NAME,
                Tables.STATE.NAMESPACE,
                Tables.STATE.STATE_,
                Tables.STATE.TYPE,
              ).values(
                UUID.randomUUID(),
                now,
                now,
                connectionId,
                streamName,
                namespace,
                jsonbState,
                stateType.convertTo<StateType>(),
              ),
          )
        } else {
          stateUpdateBatch.updatedStreamStates.add(
            ctx
              .update(Tables.STATE)
              .set(Tables.STATE.UPDATED_AT, now)
              .set(Tables.STATE.STATE_, jsonbState)
              .where(
                Tables.STATE.CONNECTION_ID.eq(connectionId),
                PersistenceHelpers.isNullOrEquals(Tables.STATE.STREAM_NAME, streamName),
                PersistenceHelpers.isNullOrEquals(Tables.STATE.NAMESPACE, namespace),
              ),
          )
        }
      } else {
        // If the state is null, we remove the state instead of keeping a null row
        stateUpdateBatch.deletedStreamStates.add(
          ctx
            .deleteFrom(Tables.STATE)
            .where(
              Tables.STATE.CONNECTION_ID.eq(connectionId),
              PersistenceHelpers.isNullOrEquals(Tables.STATE.STREAM_NAME, streamName),
              PersistenceHelpers.isNullOrEquals(Tables.STATE.NAMESPACE, namespace),
            ),
        )
      }
    }

    /**
     * Get the StateType for a given list of StateRecords.
     *
     * @param connectionId The connectionId of the records, used to add more debugging context if an
     * error is detected
     * @param records The list of StateRecords to process, must not be empty
     * @return the StateType of the records
     * @throws IllegalStateException If StateRecords have inconsistent types
     */
    private fun getStateType(
      connectionId: UUID,
      records: List<StateRecord>,
    ): StateType {
      val types =
        records.stream().map { r: StateRecord -> r.type }.collect(Collectors.toSet())
      if (types.size == 1) {
        return types.stream().findFirst().get()
      }

      throw IllegalStateException(
        (
          "Inconsistent StateTypes for connectionId " + connectionId +
            " (" + java.lang.String.join(", ", types.stream().map { obj -> obj.literal }.toList()) + ")"
        ),
      )
    }

    /**
     * Get the state records from the DB.
     *
     * @param ctx A valid DSL context to use for the query
     * @param connectionId the ID of the connection
     * @return The StateRecords for the connectionId
     */
    private fun getStateRecords(
      ctx: DSLContext,
      connectionId: UUID,
    ): List<StateRecord> =
      ctx
        .select(DSL.asterisk())
        .from(Tables.STATE)
        .where(Tables.STATE.CONNECTION_ID.eq(connectionId))
        .fetch(stateRecordMapper)
        .stream()
        .toList()

    /**
     * Delete all connection state records from the DB.
     *
     * @param ctx A valid DSL context to use for the query
     * @param connectionId the ID of the connection
     */
    private fun deleteStateRecords(
      ctx: DSLContext,
      connectionId: UUID,
    ) {
      ctx
        .deleteFrom(Tables.STATE)
        .where(Tables.STATE.CONNECTION_ID.eq(connectionId))
        .execute()
    }

    /**
     * Build Global state.
     *
     * The list of records can contain one global shared state that is the state without streamName and
     * without namespace The other records should be translated into AirbyteStreamState
     */
    private fun buildGlobalState(records: List<StateRecord>): StateWrapper {
      // Split the global shared state from the other per stream records
      val partitions =
        records
          .stream()
          .collect(
            Collectors.partitioningBy { r: StateRecord -> r.streamName == null && r.namespace == null },
          )

      val globalState =
        AirbyteGlobalState()
          .withSharedState(
            partitions[java.lang.Boolean.TRUE]!!
              .stream()
              .map { r: StateRecord -> r.state }
              .findFirst()
              .orElse(null),
          ).withStreamStates(
            partitions[java.lang.Boolean.FALSE]!!
              .stream()
              .map { record: StateRecord -> buildAirbyteStreamState(record) }
              .toList(),
          )

      val msg =
        AirbyteStateMessage()
          .withType(AirbyteStateType.GLOBAL)
          .withGlobal(globalState)
      return StateWrapper().withStateType(io.airbyte.config.StateType.GLOBAL).withGlobal(msg)
    }

    /**
     * Build StateWrapper for a PerStream state.
     *
     * @param records list of db records that comprise the full state of a connection
     * @return state wrapper
     */
    private fun buildStreamState(records: List<StateRecord>): StateWrapper {
      val messages =
        records
          .stream()
          .map { record: StateRecord ->
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(buildAirbyteStreamState(record))
          }.toList()
      return StateWrapper().withStateType(io.airbyte.config.StateType.STREAM).withStateMessages(messages)
    }

    /**
     * Build a StateWrapper for Legacy state.
     *
     * @param records list of db records that comprise the full state of a connection
     * @return state wrapper
     */
    private fun buildLegacyState(records: List<StateRecord>): StateWrapper {
      val legacyState = Jsons.convertValue(records[0].state, State::class.java)
      return StateWrapper()
        .withStateType(io.airbyte.config.StateType.LEGACY)
        .withLegacyState(legacyState.state)
    }

    /**
     * Convert a StateRecord to an AirbyteStreamState.
     *
     * @param record db record
     * @return state record
     */
    private fun buildAirbyteStreamState(record: StateRecord): AirbyteStreamState =
      AirbyteStreamState()
        .withStreamDescriptor(
          io.airbyte.protocol.models.v0
            .StreamDescriptor()
            .withName(record.streamName)
            .withNamespace(record.namespace),
        ).withStreamState(record.state)

    private val stateRecordMapper: RecordMapper<Record, StateRecord>
      get() =
        RecordMapper { record: Record ->
          StateRecord(
            record.get(
              Tables.STATE.TYPE,
              StateType::class.java,
            ),
            record.get(Tables.STATE.STREAM_NAME, String::class.java),
            record.get(Tables.STATE.NAMESPACE, String::class.java),
            Jsons.deserialize(record.get(Tables.STATE.STATE_).data()),
          )
        }
  }
}
