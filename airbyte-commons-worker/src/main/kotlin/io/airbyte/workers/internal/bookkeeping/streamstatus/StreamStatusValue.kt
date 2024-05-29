package io.airbyte.workers.internal.bookkeeping.streamstatus

import io.airbyte.api.client.model.generated.StreamStatusRunState as ApiEnum

data class StreamStatusValue(
  var runState: ApiEnum? = null,
  var latestStateId: Int? = null,
  var sourceComplete: Boolean = false,
  var streamEmpty: Boolean = true,
)
