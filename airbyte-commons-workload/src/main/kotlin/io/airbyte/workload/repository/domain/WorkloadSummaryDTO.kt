/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.repository.domain

import io.micronaut.core.annotation.Introspected
import java.time.OffsetDateTime
import java.util.UUID

/**
 * A reflection-free DTO projection for [io.airbyte.workload.repository.WorkloadRepository.searchActive].
 *
 * This is used to avoid pulling the entire workload when we are searching for active workloads for sanity check purposes.
 */
@Introspected
data class WorkloadSummaryDTO(
  var id: String,
  var status: WorkloadStatus,
  var deadline: OffsetDateTime? = null,
  var autoId: UUID? = null,
)
