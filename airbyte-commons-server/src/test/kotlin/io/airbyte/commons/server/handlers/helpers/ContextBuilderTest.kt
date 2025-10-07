/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.config.ConnectionContext
import io.airbyte.config.DestinationConnection
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardWorkspace
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.metrics.MetricClient
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class ContextBuilderTest {
  private val workspaceService = mockk<WorkspaceService>()
  private val destinationService = mockk<DestinationService>()
  private val connectionService = mockk<ConnectionService>()
  private val sourceService = mockk<SourceService>()
  private val metricClient = mockk<MetricClient>(relaxed = true)
  private val contextBuilder = ContextBuilder(workspaceService, destinationService, connectionService, sourceService, metricClient)

  private val connectionId = UUID.randomUUID()
  private val sourceId = UUID.randomUUID()
  private val destinationId = UUID.randomUUID()
  private val workspaceId = UUID.randomUUID()
  private val organizationId = UUID.randomUUID()
  private val destinationDefinitionId = UUID.randomUUID()
  private val sourceDefinitionId = UUID.randomUUID()

  @Test
  fun `test the creation of the connection context`() {
    every { connectionService.getStandardSync(connectionId) } returns
      StandardSync()
        .withConnectionId(connectionId)
        .withSourceId(sourceId)
        .withDestinationId(destinationId)

    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false) } returns
      StandardWorkspace()
        .withWorkspaceId(workspaceId)
        .withOrganizationId(organizationId)

    every { sourceService.getSourceConnection(sourceId) } returns
      SourceConnection()
        .withSourceDefinitionId(sourceDefinitionId)

    every { destinationService.getDestinationConnection(destinationId) } returns
      DestinationConnection()
        .withWorkspaceId(workspaceId)
        .withDestinationDefinitionId(destinationDefinitionId)

    val context = contextBuilder.fromConnectionId(connectionId)

    assertEquals(
      ConnectionContext()
        .withConnectionId(connectionId)
        .withSourceId(sourceId)
        .withDestinationId(destinationId)
        .withWorkspaceId(workspaceId)
        .withOrganizationId(organizationId)
        .withSourceDefinitionId(sourceDefinitionId)
        .withDestinationDefinitionId(destinationDefinitionId),
      context,
    )
  }
}
