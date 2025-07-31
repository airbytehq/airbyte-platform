/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.model

import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput

typealias OpenApiWorkload = io.airbyte.workload.api.domain.Workload
typealias OpenApiWorkloadType = io.airbyte.config.WorkloadType
typealias InternalApiWorkloadType = io.airbyte.config.WorkloadType

fun OpenApiWorkloadType.toInternalApi(): io.airbyte.config.WorkloadType =
  when (this) {
    OpenApiWorkloadType.CHECK -> InternalApiWorkloadType.CHECK
    OpenApiWorkloadType.DISCOVER -> InternalApiWorkloadType.DISCOVER
    OpenApiWorkloadType.SPEC -> InternalApiWorkloadType.SPEC
    OpenApiWorkloadType.SYNC -> InternalApiWorkloadType.SYNC
  }

fun OpenApiWorkload.toLauncherInput(): LauncherInput =
  LauncherInput(
    workloadId = this.id,
    workloadInput = this.inputPayload,
    labels = this.labels.associate { it.key to it.value },
    logPath = this.logPath,
    mutexKey = this.mutexKey,
    workloadType = this.type.toInternalApi(),
    autoId = this.autoId,
  )
