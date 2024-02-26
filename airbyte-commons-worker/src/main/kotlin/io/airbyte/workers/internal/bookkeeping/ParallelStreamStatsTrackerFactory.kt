package io.airbyte.workers.internal.bookkeeping

import io.micronaut.context.ApplicationContext
import io.micronaut.kotlin.context.createBean
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class ParallelStreamStatsTrackerFactory(private val applicationContext: ApplicationContext) {
  fun get(
    connectionId: UUID,
    workspaceId: UUID,
    jobId: Long,
    attemptNumber: Int,
  ): ParallelStreamStatsTracker = applicationContext.createBean(connectionId, workspaceId, jobId, attemptNumber)
}
