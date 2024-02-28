/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.models;

import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.config.StandardDiscoverCatalogInput;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SidecarInput {

  public enum OperationType {
    CHECK,
    DISCOVER,
    SPEC,
  }

  StandardCheckConnectionInput checkConnectionInput;
  StandardDiscoverCatalogInput discoverCatalogInput;
  String workloadId;
  IntegrationLauncherConfig integrationLauncherConfig;
  OperationType operationType;

}
