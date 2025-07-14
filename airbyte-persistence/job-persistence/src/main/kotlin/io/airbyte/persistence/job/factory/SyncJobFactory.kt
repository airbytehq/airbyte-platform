/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.factory

import io.airbyte.config.persistence.domain.StreamRefresh
import java.util.UUID

/**
 * Interface to hide implementation of creating a sync job.
 */
interface SyncJobFactory {
  /**
   * Create sync job for given connection id.
   *
   * @param connectionId connection id
   * @return job id
   */
  fun createSync(
    connectionId: UUID,
    isScheduled: Boolean,
  ): Long

  /**
   * Create refresh job for given connection id.
   *
   * @param connectionId connection id
   * @return job id
   */
  fun createRefresh(
    connectionId: UUID,
    streamsToRefresh: List<StreamRefresh>,
  ): Long
}
