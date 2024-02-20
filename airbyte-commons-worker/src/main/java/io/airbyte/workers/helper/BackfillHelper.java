/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.airbyte.api.client.model.generated.CatalogDiff;
import io.airbyte.api.client.model.generated.ConnectionRead;
import io.airbyte.api.client.model.generated.FieldTransform;
import io.airbyte.api.client.model.generated.SchemaChangeBackfillPreference;
import io.airbyte.api.client.model.generated.StreamDescriptor;
import io.airbyte.api.client.model.generated.StreamTransform;
import io.airbyte.commons.converters.CatalogClientConverters;
import io.airbyte.commons.converters.ProtocolConverters;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.State;
import io.airbyte.config.StateType;
import io.airbyte.config.StateWrapper;
import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.helpers.StateMessageHelper;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.SyncMode;
import io.airbyte.workers.models.ReplicationActivityInput;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class BackfillHelper {

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
  public static boolean syncShouldBackfill(final ReplicationActivityInput replicationActivityInput, final ConnectionRead connectionInfo) {
    final boolean backfillEnabledForConnection =
        connectionInfo.getBackfillPreference() != null && connectionInfo.getBackfillPreference().equals(SchemaChangeBackfillPreference.ENABLED);
    final boolean hasSchemaDiff =
        replicationActivityInput.getSchemaRefreshOutput() != null && replicationActivityInput.getSchemaRefreshOutput().getAppliedDiff() != null
            && !replicationActivityInput.getSchemaRefreshOutput().getAppliedDiff().getTransforms().isEmpty();
    final boolean schemaDiffNeedsBackfill =
        hasSchemaDiff && atLeastOneStreamNeedsBackfill(replicationActivityInput.getSchemaRefreshOutput().getAppliedDiff(), connectionInfo);
    return backfillEnabledForConnection && hasSchemaDiff && schemaDiffNeedsBackfill;
  }

  /**
   * For the listed streams, set their state to null. Returns the modified state
   *
   * @param inputState the state to be modified
   * @param streamsToBackfill the list of streams that need backfill
   * @return the modified state if any streams were cleared, else null
   */
  public static State clearStateForStreamsToBackfill(State inputState, List<StreamDescriptor> streamsToBackfill) {
    if (inputState == null) {
      // This would be the case for a Full Refresh sync.
      return null;
    }
    final var stateOptional = StateMessageHelper.getTypedState(inputState.getState());
    if (!stateOptional.isPresent()) {
      return null; // No state, no backfill.
    }
    final StateWrapper state = stateOptional.get();
    final StateType type = state.getStateType();
    if (!StateType.STREAM.equals(type)) {
      return null; // Only backfill for per-stream state.
    }
    boolean stateWasModified = false;
    for (final var stateMessage : state.getStateMessages()) {
      if (!AirbyteStateMessage.AirbyteStateType.STREAM.equals(stateMessage.getType())) {
        continue;
      }
      if (!streamsToBackfill.contains(
          ProtocolConverters.streamDescriptorToClient(stateMessage.getStream().getStreamDescriptor()))) {
        continue;
      }
      // It's listed in the streams to backfill, so we write the state to null.
      stateMessage.getStream().setStreamState(JsonNodeFactory.instance.nullNode());
      stateWasModified = true;
    }
    return stateWasModified ? StateMessageHelper.getState(state) : null;
  }

  /**
   * Given a catalog diff and a configured catalog, identifies the streams that and candidates for
   * backfill.
   *
   * @param appliedDiff the diff that was applied since the last sync
   * @param catalog the entire catalog
   * @return any streams that need to be backfilled
   */
  public static List<StreamDescriptor> getStreamsToBackfill(CatalogDiff appliedDiff, ConfiguredAirbyteCatalog catalog) {
    if (appliedDiff == null || appliedDiff.getTransforms().isEmpty()) {
      // No diff, so no streams to backfill.
      return List.of();
    }
    final List<StreamDescriptor> streamsToBackfill = new ArrayList<>();
    appliedDiff.getTransforms().forEach(transform -> {
      if (StreamTransform.TransformTypeEnum.UPDATE_STREAM.equals(transform.getTransformType()) && shouldBackfillStream(transform, catalog)) {
        streamsToBackfill.add(transform.getStreamDescriptor());
      }
    });
    return streamsToBackfill;
  }

  /**
   * Indicate which, if any, streams were backfilled. We track these separately, since they were
   * platform-initiated.
   *
   * @param streamsToBackfill the streams to backfill
   * @param syncOutput output param, where we indicate the backfilled streams
   */
  public static void markBackfilledStreams(List<StreamDescriptor> streamsToBackfill, StandardSyncOutput syncOutput) {
    if (syncOutput.getStandardSyncSummary().getStreamStats() == null) {
      return; // No stream stats, no backfill.
    }
    if (streamsToBackfill == null) {
      return; // No streams to backfill, no backfill.
    }
    for (final StreamSyncStats streamStat : syncOutput.getStandardSyncSummary().getStreamStats()) {
      if (streamsToBackfill.contains(new StreamDescriptor().name(streamStat.getStreamName()).namespace(streamStat.getStreamNamespace()))) {
        streamStat.setWasBackfilled(true);
      }
    }
  }

  private static boolean atLeastOneStreamNeedsBackfill(final CatalogDiff appliedDiff, final ConnectionRead connectionInfo) {
    return !getStreamsToBackfill(appliedDiff, CatalogClientConverters.toConfiguredAirbyteProtocol(connectionInfo.getSyncCatalog())).isEmpty();
  }

  private static boolean shouldBackfillStream(final StreamTransform transform, final ConfiguredAirbyteCatalog catalog) {

    final var streamOptional = catalog.getStreams().stream().filter(
        stream -> {
          String streamName = stream.getStream().getName();
          String streamNamespace = Optional.ofNullable(stream.getStream().getNamespace()).orElse("");
          String transformNamespace = Optional.ofNullable(transform.getStreamDescriptor().getNamespace()).orElse("");

          return streamName.equals(transform.getStreamDescriptor().getName())
              && streamNamespace.equals(transformNamespace);
        }).findFirst();

    if (streamOptional.isEmpty()) {
      // This should never happen, and we should eventually throw an error, but for now just return false.
      return false;
    }
    final ConfiguredAirbyteStream stream = streamOptional.get();
    if (!SyncMode.INCREMENTAL.equals(stream.getSyncMode())) {
      // Only backfill incremental streams, since Full Refresh streams and pulling the whole history
      // anyway.
      return false;
    }
    for (FieldTransform fieldTransform : transform.getUpdateStream()) {
      // TODO: we'll add other cases here, when we develop the config options further.
      if (FieldTransform.TransformTypeEnum.ADD_FIELD.equals(fieldTransform.getTransformType())) {
        return true;
      }
      if (FieldTransform.TransformTypeEnum.UPDATE_FIELD_SCHEMA.equals(fieldTransform.getTransformType())) {
        return true;
      }
    }
    return false;
  }

}
