/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping.streamstatus

import io.airbyte.api.client.model.generated.StreamStatusRateLimitedMetadata
import io.airbyte.api.client.model.generated.StreamStatusRunState as ApiEnum

data class StreamStatusValue(
  var runState: ApiEnum? = null,
  var latestStateId: Int? = null,
  var sourceComplete: Boolean = false,
  var streamEmpty: Boolean = true,
  var metadata: StreamStatusRateLimitedMetadata? = null,
)
