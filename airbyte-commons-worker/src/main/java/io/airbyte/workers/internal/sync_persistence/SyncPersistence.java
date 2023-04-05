/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.sync_persistence;

import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.workers.internal.book_keeping.SyncStatsTracker;
import java.util.UUID;

/**
 * Handles persistence that needs to happen during a replication.
 */
public interface SyncPersistence extends SyncStatsTracker, AutoCloseable {

  /**
   * Set the required context for sync persistence.
   *
   * @param connectionId the Connection of the current Sync
   * @param jobId the JobId of the current Sync
   * @param attemptNumber the AttemptNumber of the current Sync
   * @param configuredAirbyteCatalog the ConfiguredAirbyteCatalog to use in case of a state migration
   *        validation. State migration validation should be needed if we are migration from a LEGACY
   *        state to a STREAM state.
   */
  void setConnectionContext(final UUID connectionId,
                            final Long jobId,
                            final Integer attemptNumber,
                            final ConfiguredAirbyteCatalog configuredAirbyteCatalog);

  /**
   * Persist a state for a given connectionId.
   *
   * @param connectionId the connection
   * @param stateMessage stateMessage to persist
   */
  void persist(final UUID connectionId, final AirbyteStateMessage stateMessage);

}
