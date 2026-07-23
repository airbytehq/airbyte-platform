/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.domain

import java.time.OffsetDateTime

data class ExpiredDeadlineWorkloadListRequest(
  var dataplane: List<String>? = null,
  var status: List<WorkloadStatus>? = null,
  var deadline: OffsetDateTime,
)
