/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.StreamDescriptor
import io.airbyte.data.repositories.specialized.LastJobWithStatsPerStreamRepository
import io.airbyte.data.services.StreamStatusesService
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class StreamStatusesServiceDataImpl(
  private val lastJobWithStatsPerStreamRepository: LastJobWithStatsPerStreamRepository,
) : StreamStatusesService {
  override fun getLastJobIdWithStatsByStream(connectionId: UUID): Map<StreamDescriptor, Long> {
    val result = lastJobWithStatsPerStreamRepository.findLastJobIdWithStatsPerStream(connectionId)
    return result.associate { StreamDescriptor().withName(it.streamName).withNamespace(it.streamNamespace) to it.jobId }
  }
}
