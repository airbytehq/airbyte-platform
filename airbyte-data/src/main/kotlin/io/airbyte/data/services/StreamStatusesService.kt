package io.airbyte.data.services

import io.airbyte.config.StreamDescriptor
import java.util.UUID

interface StreamStatusesService {
  fun getLastJobIdWithStatsByStream(connectionId: UUID): Map<StreamDescriptor, Long>
}
