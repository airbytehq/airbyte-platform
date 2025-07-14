/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config

import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.persistence.job.WorkspaceHelper
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton

/**
 * Bean factory for workspace helper.
 */
@Factory
class HelperBeanFactory {
  @Singleton
  fun workspaceHelper(
    jobPersistence: JobPersistence,
    connectionService: ConnectionService,
    sourceService: SourceService,
    destinationService: DestinationService,
    operationService: OperationService,
    workspaceService: WorkspaceService,
  ): WorkspaceHelper = WorkspaceHelper(jobPersistence, connectionService, sourceService, destinationService, operationService, workspaceService)
}
