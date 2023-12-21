/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.models;

import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import java.util.HashMap;
import java.util.Map;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

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

  @Setter(AccessLevel.NONE)
  private Map<String, String> labels = new HashMap<>();

  public CheckConnectionInput(JobRunConfig jobRunConfig,
                              IntegrationLauncherConfig launcherConfig,
                              StandardCheckConnectionInput connectionConfiguration) {
    this.jobRunConfig = jobRunConfig;
    this.launcherConfig = launcherConfig;
    this.connectionConfiguration = connectionConfiguration;
  }

  public void addLabel(final String key, final String value) {
    labels.put(key, value);
  }

  public void addLabels(final Map<String, String> labels) {
    this.labels.putAll(labels);
  }

}
