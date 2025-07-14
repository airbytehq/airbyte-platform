/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.airbyte.api.client.model.generated.ConnectionRead
import io.airbyte.api.client.model.generated.SchemaChangeBackfillPreference
import io.airbyte.commons.converters.CatalogClientConverters
import io.airbyte.config.CatalogDiff
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.FieldTransform
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.State
import io.airbyte.config.StateType
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.StreamTransform
import io.airbyte.config.SyncMode
import io.airbyte.config.helpers.ProtocolConverters.Companion.toInternal
import io.airbyte.config.helpers.StateMessageHelper.getState
import io.airbyte.config.helpers.StateMessageHelper.getTypedState
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.workers.models.ReplicationActivityInput
import jakarta.inject.Singleton
import java.util.Optional
import java.util.function.Consumer

@Singleton
class BackfillHelper(
  private val catalogClientConverters: CatalogClientConverters,
) {
  /**
   * Indicates whether the current sync replication activity should perform a backfill. A backfill
   * happens under the following conditions: - The feature is enabled for the connection. - There is a
   * schema diff with a new or updated field. NOTE: these conditions might change or expand in the
   * future.
   *
   * @param replicationActivityInput the input for the sync replication
   * @param connectionInfo details about the connection to determine whether backfill is enabled
   * @return true if at least one stream should be backfilled
   */
  fun syncShouldBackfill(
    replicationActivityInput: ReplicationActivityInput,
    connectionInfo: ConnectionRead,
  ): Boolean {
    val backfillEnabledForConnection =
      connectionInfo.backfillPreference != null && connectionInfo.backfillPreference == SchemaChangeBackfillPreference.ENABLED
    val hasSchemaDiff =
      replicationActivityInput.schemaRefreshOutput != null &&
        replicationActivityInput.schemaRefreshOutput!!.appliedDiff != null &&
        !replicationActivityInput.schemaRefreshOutput!!
          .appliedDiff!!
          .transforms
          .isEmpty()
    val schemaDiffNeedsBackfill =
      hasSchemaDiff &&
        atLeastOneStreamNeedsBackfill(replicationActivityInput.schemaRefreshOutput!!.appliedDiff, connectionInfo)
    return backfillEnabledForConnection && hasSchemaDiff && schemaDiffNeedsBackfill
  }

  /**
   * For the listed streams, set their state to null. Returns the modified state
   *
   * @param inputState the state to be modified
   * @param streamsToBackfill the list of streams that need backfill
   * @return the modified state if any streams were cleared, else null
   */
  fun clearStateForStreamsToBackfill(
    inputState: State?,
    streamsToBackfill: List<StreamDescriptor?>,
  ): State? {
    if (inputState == null) {
      // This would be the case for a Full Refresh sync.
      return null
    }
    val stateOptional = getTypedState(inputState.state)
    if (stateOptional.isEmpty) {
      return null // No state, no backfill.
    }
    val state = stateOptional.get()
    val type = state.stateType
    if (StateType.STREAM != type) {
      return null // Only backfill for per-stream state.
    }
    var stateWasModified = false
    for (stateMessage in state.stateMessages) {
      if (AirbyteStateMessage.AirbyteStateType.STREAM != stateMessage.type) {
        continue
      }
      if (!streamsToBackfill.contains(
          stateMessage.stream.streamDescriptor.toInternal(),
        )
      ) {
        continue
      }
      // It's listed in the streams to backfill, so we write the state to null.
      stateMessage.stream.streamState = JsonNodeFactory.instance.nullNode()
      stateWasModified = true
    }
    return if (stateWasModified) getState(state) else null
  }

  /**
   * Given a catalog diff and a configured catalog, identifies the streams that are candidates for
   * backfill.
   *
   * @param appliedDiff the diff that was applied since the last sync
   * @param catalog the entire catalog
   * @return any streams that need to be backfilled
   */
  fun getStreamsToBackfill(
    appliedDiff: CatalogDiff?,
    catalog: ConfiguredAirbyteCatalog,
  ): List<StreamDescriptor> {
    if (appliedDiff == null || appliedDiff.transforms.isEmpty()) {
      // No diff, so no streams to backfill.
      return listOf()
    }
    val streamsToBackfill: MutableList<StreamDescriptor> = ArrayList()
    appliedDiff.transforms.forEach(
      Consumer { transform: StreamTransform ->
        if (StreamTransform.TransformType.UPDATE_STREAM == transform.transformType && shouldBackfillStream(transform, catalog)) {
          streamsToBackfill.add(transform.streamDescriptor)
        }
      },
    )
    return streamsToBackfill
  }

  /**
   * Indicate which, if any, streams were backfilled. We track these separately, since they were
   * platform-initiated.
   *
   * @param streamsToBackfill the streams to backfill
   * @param syncOutput output param, where we indicate the backfilled streams
   */
  fun markBackfilledStreams(
    streamsToBackfill: List<StreamDescriptor?>?,
    syncOutput: StandardSyncOutput,
  ) {
    if (syncOutput.standardSyncSummary.streamStats == null) {
      return // No stream stats, no backfill.
    }
    if (streamsToBackfill == null) {
      return // No streams to backfill, no backfill.
    }
    for (streamStat in syncOutput.standardSyncSummary.streamStats) {
      if (streamsToBackfill.contains(StreamDescriptor().withName(streamStat.streamName).withNamespace(streamStat.streamNamespace))) {
        streamStat.wasBackfilled = true
      }
    }
  }

  private fun atLeastOneStreamNeedsBackfill(
    appliedDiff: CatalogDiff?,
    connectionInfo: ConnectionRead,
  ): Boolean {
    val configuredCatalog =
      catalogClientConverters.toConfiguredAirbyteInternal(connectionInfo.syncCatalog)
    return !getStreamsToBackfill(appliedDiff, configuredCatalog).isEmpty()
  }

  private fun shouldBackfillStream(
    transform: StreamTransform,
    catalog: ConfiguredAirbyteCatalog,
  ): Boolean {
    val streamOptional =
      catalog.streams
        .stream()
        .filter { stream: ConfiguredAirbyteStream ->
          val streamName = stream.stream.name
          val streamNamespace = Optional.ofNullable(stream.stream.namespace).orElse("")
          val transformNamespace = Optional.ofNullable(transform.streamDescriptor.namespace).orElse("")
          streamName == transform.streamDescriptor.name &&
            streamNamespace == transformNamespace
        }.findFirst()

    if (streamOptional.isEmpty) {
      // This should never happen, and we should eventually throw an error, but for now just return false.
      return false
    }
    val stream = streamOptional.get()
    if (SyncMode.INCREMENTAL != stream.syncMode) {
      // Only backfill incremental streams, since Full Refresh streams are pulling the whole history
      // anyway.
      return false
    }
    for (fieldTransform in transform.updateStream.fieldTransforms) {
      // TODO: we'll add other cases here when we develop the config options further.
      if (FieldTransform.TransformType.ADD_FIELD == fieldTransform.transformType) {
        return true
      }
      if (FieldTransform.TransformType.UPDATE_FIELD_SCHEMA == fieldTransform.transformType) {
        return true
      }
    }
    return false
  }
}
