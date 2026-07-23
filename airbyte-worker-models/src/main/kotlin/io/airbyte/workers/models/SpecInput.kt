/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.models

import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig

data class SpecInput(
  var jobRunConfig: JobRunConfig,
  var launcherConfig: IntegrationLauncherConfig,
)
