/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.domain

import java.time.OffsetDateTime

data class LongRunningWorkloadRequest(
  var dataplane: List<String>? = null,
  var createdBefore: OffsetDateTime? = null,
)
