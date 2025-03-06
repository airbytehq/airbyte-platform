/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.bookkeeping

import io.airbyte.workers.config.StateCheckSumCountEventHandlerFactory
import io.micronaut.context.ApplicationContext
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class ParallelStreamStatsTrackerFactory(
  private val applicationContext: ApplicationContext,
  private val stateCheckSumCountEventHandlerFactory: StateCheckSumCountEventHandlerFactory,
) {
  fun get(
    connectionId: UUID,
    workspaceId: UUID,
    jobId: Long,
    attemptNumber: Int,
  ): ParallelStreamStatsTracker {
    val stateCheckSumCountEventHandler = stateCheckSumCountEventHandlerFactory.get(connectionId, workspaceId, jobId, attemptNumber)
    return applicationContext.createBean(ParallelStreamStatsTracker::class.java, stateCheckSumCountEventHandler)
  }
}
