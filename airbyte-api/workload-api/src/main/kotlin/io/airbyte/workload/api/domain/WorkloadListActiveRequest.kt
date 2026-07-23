/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.domain

data class WorkloadListActiveRequest(
  var dataplane: List<String>? = null,
  var status: List<WorkloadStatus>? = null,
)
