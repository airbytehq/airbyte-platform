/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config;

import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.WorkspaceHelper;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;

/**
 * Bean factory for workspace helper.
 */
@Factory
public class HelperBeanFactory {

  @Singleton
  public WorkspaceHelper workspaceHelper(final ConfigRepository configRepository, final JobPersistence jobPersistence) {
    return new WorkspaceHelper(configRepository, jobPersistence);
  }

}
