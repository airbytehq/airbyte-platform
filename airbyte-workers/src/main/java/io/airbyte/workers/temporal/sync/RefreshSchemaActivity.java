/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import io.airbyte.workers.models.RefreshSchemaActivityInput;
import io.airbyte.workers.models.RefreshSchemaActivityOutput;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.util.UUID;

/**
 * RefreshSchemaActivity.
 */
@ActivityInterface
public interface RefreshSchemaActivity {

  @ActivityMethod
  boolean shouldRefreshSchema(UUID sourceCatalogId);

  void refreshSchema(UUID sourceCatalogId, UUID connectionId) throws Exception;

  /**
   * Refresh the schema. This will eventually replace the one above.
   *
   * @param input includes the source catalog id, connection id, and workspace id
   * @return any diff that was auto-propagated
   */
  RefreshSchemaActivityOutput refreshSchemaV2(final RefreshSchemaActivityInput input) throws Exception;

}
