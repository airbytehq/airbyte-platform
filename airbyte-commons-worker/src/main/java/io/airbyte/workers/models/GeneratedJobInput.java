/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.models;

import io.airbyte.config.StandardSyncInput;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Generated job input.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class GeneratedJobInput {

  private JobRunConfig jobRunConfig;
  private IntegrationLauncherConfig sourceLauncherConfig;
  private IntegrationLauncherConfig destinationLauncherConfig;
  private StandardSyncInput syncInput;

}
