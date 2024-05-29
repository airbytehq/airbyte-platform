package io.airbyte.workers.internal.bookkeeping.events

import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusKey
import io.airbyte.api.client2.model.generated.StreamStatusRunState as ApiEnum

data class StreamStatusUpdateEvent(
  val key: StreamStatusKey,
  val runState: ApiEnum,
)
