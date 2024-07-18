/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.discover.catalog;

import io.airbyte.workers.models.PostprocessCatalogInput;
import io.airbyte.workers.models.PostprocessCatalogOutput;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.util.UUID;

@ActivityInterface
public interface DiscoverCatalogHelperActivity {

  @ActivityMethod
  boolean shouldUseWorkload(final UUID workspaceId);

  @ActivityMethod
  void reportSuccess(final Boolean workloadEnabled);

  @ActivityMethod
  void reportFailure(final Boolean workloadEnabled);

  /**
   * Perform catalog diffing, subsequent disabling of the connection and any other necessary
   * operations after performing the discover.
   */
  @ActivityMethod
  PostprocessCatalogOutput postprocess(final PostprocessCatalogInput input);

}
