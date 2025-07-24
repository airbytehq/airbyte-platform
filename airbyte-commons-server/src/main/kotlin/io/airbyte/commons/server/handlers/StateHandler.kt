/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.ConnectionState
import io.airbyte.api.model.generated.ConnectionStateCreateOrUpdate
import io.airbyte.commons.converters.StateConverter.toApi
import io.airbyte.commons.converters.StateConverter.toInternal
import io.airbyte.commons.server.errors.SyncIsRunningException
import io.airbyte.config.StateWrapper
import io.airbyte.config.persistence.StatePersistence
import io.airbyte.metrics.lib.ApmTraceUtils.addTagsToTrace
import io.airbyte.metrics.lib.MetricTags
import jakarta.inject.Singleton
import java.io.IOException
import java.util.Map
import java.util.Optional
import java.util.UUID

/**
 * StateHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
open class StateHandler(
  private val statePersistence: StatePersistence,
  private val jobHistoryHandler: JobHistoryHandler,
) {
  @Throws(IOException::class)
  fun getState(connectionIdRequestBody: ConnectionIdRequestBody): ConnectionState {
    val connectionId = connectionIdRequestBody.connectionId
    val currentState: Optional<StateWrapper> = statePersistence.getCurrentState(connectionId)
    return toApi(connectionId, currentState.orElse(null))
  }

  @Throws(IOException::class)
  fun createOrUpdateState(connectionStateCreateOrUpdate: ConnectionStateCreateOrUpdate): ConnectionState {
    val connectionId = connectionStateCreateOrUpdate.connectionId
    addTagsToTrace(Map.of(MetricTags.CONNECTION_ID, connectionId))

    val convertedCreateOrUpdate = toInternal(connectionStateCreateOrUpdate.connectionState)
    statePersistence.updateOrCreateState(connectionId, convertedCreateOrUpdate)
    val newInternalState: Optional<StateWrapper> = statePersistence.getCurrentState(connectionId)

    return toApi(connectionId, newInternalState.orElse(null))
  }

  @Throws(IOException::class)
  fun createOrUpdateStateSafe(connectionStateCreateOrUpdate: ConnectionStateCreateOrUpdate): ConnectionState {
    if (jobHistoryHandler.getLatestRunningSyncJob(connectionStateCreateOrUpdate.connectionId).isPresent) {
      throw SyncIsRunningException("State cannot be updated while a sync is running for this connection.")
    }

    return createOrUpdateState(connectionStateCreateOrUpdate)
  }

  @Throws(IOException::class)
  fun wipeState(connectionId: UUID) {
    statePersistence.eraseState(connectionId)
  }
}
