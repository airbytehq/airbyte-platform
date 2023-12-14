/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.models;

import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * CheckConnectionInput.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CheckConnectionInput {

  private JobRunConfig jobRunConfig;
  private IntegrationLauncherConfig launcherConfig;
  private StandardCheckConnectionInput connectionConfiguration;

}
