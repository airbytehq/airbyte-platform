/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.models;

import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * GeneratedJobInput.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SyncJobCheckConnectionInputs {

  private IntegrationLauncherConfig sourceLauncherConfig;
  private IntegrationLauncherConfig destinationLauncherConfig;
  private StandardCheckConnectionInput sourceCheckConnectionInput;
  private StandardCheckConnectionInput destinationCheckConnectionInput;

}
