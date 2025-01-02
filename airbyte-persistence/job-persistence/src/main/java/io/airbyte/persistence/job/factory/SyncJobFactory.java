/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.factory;

import io.airbyte.config.persistence.domain.StreamRefresh;
import java.util.List;
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
  Long createSync(UUID connectionId);

  /**
   * Create refresh job for given connection id.
   *
   * @param connectionId connection id
   * @return job id
   */
  Long createRefresh(UUID connectionId, List<StreamRefresh> streamsToRefresh);

}
