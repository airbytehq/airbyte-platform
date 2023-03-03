/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.sync_persistence;

import io.micronaut.context.ApplicationContext;
import jakarta.inject.Singleton;

/**
 * A Factory for SyncPersistence
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

  public SyncPersistence get() {
    return applicationContext.getBean(SyncPersistence.class);
  }

}
