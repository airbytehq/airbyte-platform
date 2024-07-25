/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.models;

import io.airbyte.config.StandardDiscoverCatalogInput;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@SuppressWarnings("PMD.AvoidFieldNameMatchingTypeName")
public class DiscoverCatalogInput {

  private JobRunConfig jobRunConfig;
  private IntegrationLauncherConfig launcherConfig;
  private StandardDiscoverCatalogInput discoverCatalogInput;

}
