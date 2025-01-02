/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config;

import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.OperationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
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
  public WorkspaceHelper workspaceHelper(final JobPersistence jobPersistence,
                                         final ConnectionService connectionService,
                                         final SourceService sourceService,
                                         final DestinationService destinationService,
                                         final OperationService operationService,
                                         final WorkspaceService workspaceService) {
    return new WorkspaceHelper(jobPersistence, connectionService, sourceService, destinationService, operationService, workspaceService);
  }

}
