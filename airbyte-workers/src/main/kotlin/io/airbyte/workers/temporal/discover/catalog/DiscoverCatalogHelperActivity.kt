/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.discover.catalog

import io.airbyte.workers.models.PostprocessCatalogInput
import io.airbyte.workers.models.PostprocessCatalogOutput
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod

@ActivityInterface
interface DiscoverCatalogHelperActivity {
  /**
   * Perform catalog diffing, subsequent disabling of the connection and any other necessary
   * operations after performing the discover.
   */
  @ActivityMethod
  fun postprocess(input: PostprocessCatalogInput): PostprocessCatalogOutput
}
