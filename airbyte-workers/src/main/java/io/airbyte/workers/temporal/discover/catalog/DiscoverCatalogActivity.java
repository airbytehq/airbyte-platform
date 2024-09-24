/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.discover.catalog;

import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.models.DiscoverCatalogInput;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * DiscoverCatalogActivity.
 */
@ActivityInterface
public interface DiscoverCatalogActivity {

  @ActivityMethod
  ConnectorJobOutput runWithWorkload(final DiscoverCatalogInput input) throws WorkerException;

}
