/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.sync_persistence;

import io.airbyte.protocol.models.AirbyteStateMessage;
import java.util.UUID;

/**
 * Handles persistence that needs to happen during a replication.
 */
public interface SyncPersistence extends AutoCloseable {

  void persist(final UUID connectionId, final AirbyteStateMessage stateMessage);

}
