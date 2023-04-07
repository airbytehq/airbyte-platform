/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.sync_persistence;

import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Singleton;
import java.util.UUID;

/**
 * A Factory for SyncPersistence.
 * <p>
 * Because we currently have two execution path with two different lifecycles, one being the
 * duration of a sync, the other duration of a worker. We introduce a Factory to keep an explicit
 * control of the lifecycle of this bean.
 * <p>
 * We use this factory rather to avoid having to directly inject ApplicationContext into classes
 * that would need to instantiate a SyncPersistence.
 */
@Singleton
public class SyncPersistenceFactory {

  private final ApplicationContext applicationContext;

  public SyncPersistenceFactory(final ApplicationContext applicationContext) {
    this.applicationContext = applicationContext;
  }

  /**
   * Get an instance of SyncPersistence.
   */
  public SyncPersistence get(final UUID connectionId,
                             final Long jobId,
                             final Integer attemptNumber,
                             final ConfiguredAirbyteCatalog configuredAirbyteCatalog) {
    final SyncPersistence syncPersistence = applicationContext.getBean(SyncPersistence.class);
    syncPersistence.setConnectionContext(connectionId, jobId, attemptNumber, configuredAirbyteCatalog);
    return syncPersistence;
  }

}
