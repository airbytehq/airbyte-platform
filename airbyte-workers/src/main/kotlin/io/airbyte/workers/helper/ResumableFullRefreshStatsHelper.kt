package io.airbyte.workers.helper

import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StateType
import io.airbyte.config.StateWrapper
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.StreamSyncStats
import io.airbyte.config.helpers.StateMessageHelper
import io.airbyte.persistence.job.models.ReplicationInput
import jakarta.inject.Singleton

/**
 * Helper that mutates the StandardSyncOutput to keep track of whether a stream was resumed.
 */
@Singleton
class ResumableFullRefreshStatsHelper {
  fun markResumedStreams(
    hydratedInput: ReplicationInput,
    standardSyncOutput: StandardSyncOutput,
  ) {
    val streamsWithStates: Set<StreamDescriptor> =
      StateMessageHelper
        .getTypedState(hydratedInput.state?.state)
        .map(this::getStreams).orElse(listOf())
        .toSet()

    standardSyncOutput.standardSyncSummary?.streamStats?.let {
      it
        .filter { s -> streamsWithStates.contains(s.streamDescriptor()) }
        .map { s -> s.wasResumed = true }
    }
  }

  private fun getStreams(stateWrapper: StateWrapper): List<StreamDescriptor> {
    return when (stateWrapper.stateType) {
      StateType.STREAM -> stateWrapper.stateMessages.map { it.stream.streamDescriptor }
      StateType.GLOBAL -> stateWrapper.global.global.streamStates.map { s -> s.streamDescriptor }
      else -> throw IllegalStateException("Legacy states are no longer supported")
    }.map { it.toConfigObject() }
  }

  private fun StreamSyncStats.streamDescriptor(): StreamDescriptor = StreamDescriptor().withName(this.streamName).withNamespace(this.streamNamespace)

  private fun io.airbyte.protocol.models.StreamDescriptor.toConfigObject(): StreamDescriptor =
    StreamDescriptor().withName(this.name).withNamespace(this.namespace)
}
