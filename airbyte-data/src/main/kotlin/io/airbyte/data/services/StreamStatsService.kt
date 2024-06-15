package io.airbyte.data.services

import io.airbyte.config.StreamDescriptor
import java.util.UUID

interface StreamStatsService {
  fun getLastJobIdWithStatsByStream(
    connectionId: UUID,
    streams: Collection<StreamDescriptor>,
  ): Map<StreamDescriptor, Long>
}
