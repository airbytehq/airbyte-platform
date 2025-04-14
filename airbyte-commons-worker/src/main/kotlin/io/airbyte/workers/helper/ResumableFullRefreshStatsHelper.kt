/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper

import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.State
import io.airbyte.config.StateType
import io.airbyte.config.StateWrapper
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.StreamSyncStats
import io.airbyte.config.SyncMode
import io.airbyte.config.helpers.StateMessageHelper
import io.airbyte.persistence.job.models.ReplicationInput
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger { }

/**
 * Helper that mutates the StandardSyncOutput to keep track of whether a stream was resumed.
 */
@Singleton
class ResumableFullRefreshStatsHelper {
  fun markResumedStreams(
    hydratedInput: ReplicationInput,
    standardSyncOutput: StandardSyncOutput,
  ) {
    val streamsWithStates: Set<StreamDescriptor> = getStreamsWithStates(hydratedInput.state)

    standardSyncOutput.standardSyncSummary?.streamStats?.let {
      it
        .filter { s -> streamsWithStates.contains(s.streamDescriptor()) }
        .map { s -> s.wasResumed = true }
    }
  }

  fun getResumedFullRefreshStreams(
    catalog: ConfiguredAirbyteCatalog,
    state: State?,
  ): Set<StreamDescriptor> {
    val streamsWithStates: Set<StreamDescriptor> = getStreamsWithStates(state)

    val fullRefreshStreams =
      catalog.streams
        .filter { s -> s.syncMode == SyncMode.FULL_REFRESH }
        .map { s -> StreamDescriptor().withNamespace(s.stream.namespace).withName(s.stream.name) }
        .toSet()

    return streamsWithStates intersect fullRefreshStreams
  }

  fun getStreamsWithStates(state: State?): Set<StreamDescriptor> =
    StateMessageHelper
      .getTypedState(state?.state)
      .map(this::getStreams)
      .orElse(listOf())
      .toSet()

  private fun getStreams(stateWrapper: StateWrapper): List<StreamDescriptor> =
    when (stateWrapper.stateType) {
      StateType.STREAM -> stateWrapper.stateMessages.map { it.stream.streamDescriptor }
      StateType.GLOBAL ->
        stateWrapper.global.global.streamStates
          .map { s -> s.streamDescriptor }
      else -> {
        logger.warn { "Legacy states are no longer supported" }
        listOf()
      }
    }.map { it.toConfigObject() }

  private fun StreamSyncStats.streamDescriptor(): StreamDescriptor = StreamDescriptor().withName(this.streamName).withNamespace(this.streamNamespace)

  private fun io.airbyte.protocol.models.v0.StreamDescriptor.toConfigObject(): StreamDescriptor =
    StreamDescriptor().withName(this.name).withNamespace(this.namespace)
}
