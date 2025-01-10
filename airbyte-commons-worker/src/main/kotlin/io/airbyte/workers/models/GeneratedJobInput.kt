/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.workers.models

import io.airbyte.config.StandardSyncInput
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig

/**
 * Generated job input.
 */
data class GeneratedJobInput(
  private var jobRunConfig: JobRunConfig? = null,
  private var sourceLauncherConfig: IntegrationLauncherConfig? = null,
  private var destinationLauncherConfig: IntegrationLauncherConfig? = null,
  private var syncInput: StandardSyncInput? = null,
)
