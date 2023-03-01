/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.factory;

import java.util.UUID;

/**
 * Interface to hide implementation of creating a sync job.
 */
public interface SyncJobFactory {

  /**
   * Create sync job for given connection id.
   *
   * @param connectionId connection id
   * @return job id
   */
  Long create(UUID connectionId);

}
