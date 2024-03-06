/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.discover.catalog;

import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.StandardDiscoverCatalogInput;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.models.DiscoverCatalogInput;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.util.UUID;

/**
 * DiscoverCatalogActivity.
 */
@ActivityInterface
public interface DiscoverCatalogActivity {

  @ActivityMethod
  ConnectorJobOutput run(JobRunConfig jobRunConfig,
                         IntegrationLauncherConfig launcherConfig,
                         StandardDiscoverCatalogInput config);

  @ActivityMethod
  ConnectorJobOutput runWithWorkload(final DiscoverCatalogInput input) throws WorkerException;

  @ActivityMethod
  boolean shouldUseWorkload(final UUID workspaceId);

  @ActivityMethod
  void reportSuccess(final Boolean workloadEnabled);

  @ActivityMethod
  void reportFailure(final Boolean workloadEnabled);

}
