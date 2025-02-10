/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.input

import io.airbyte.config.ActorContext
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectionContext
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.Destination
import io.airbyte.featureflag.DestinationDefinition
import io.airbyte.featureflag.Empty
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.Source
import io.airbyte.featureflag.SourceDefinition
import io.airbyte.featureflag.Workspace
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.input.FeatureFlagContextExtensionsTest.Fixtures.connectionId
import io.airbyte.workers.input.FeatureFlagContextExtensionsTest.Fixtures.destinationDefinitionId
import io.airbyte.workers.input.FeatureFlagContextExtensionsTest.Fixtures.destinationId
import io.airbyte.workers.input.FeatureFlagContextExtensionsTest.Fixtures.organizationId
import io.airbyte.workers.input.FeatureFlagContextExtensionsTest.Fixtures.sourceDefinitionId
import io.airbyte.workers.input.FeatureFlagContextExtensionsTest.Fixtures.sourceId
import io.airbyte.workers.input.FeatureFlagContextExtensionsTest.Fixtures.workspaceId
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SpecInput
import io.mockk.every
import io.mockk.mockkStatic
import io.mockk.unmockkStatic
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class FeatureFlagContextExtensionsTest {
  @AfterEach
  fun cleanup() {
    unmockkStatic(IntegrationLauncherConfig::toFeatureFlagContext)
    unmockkStatic(ConnectionContext::toFeatureFlagContext)
  }

  @Test
  fun `ActorContext#toFeatureFlagContext happy path source`() {
    val input =
      ActorContext()
        .withWorkspaceId(workspaceId)
        .withOrganizationId(organizationId)
        .withActorId(sourceId)
        .withActorDefinitionId(sourceDefinitionId)
        .withActorType(ActorType.SOURCE)

    val result = input.toFeatureFlagContext()

    val expected =
      Multi(
        listOf(
          Workspace(workspaceId),
          Organization(organizationId),
          Source(sourceId),
          SourceDefinition(sourceDefinitionId),
        ),
      )

    assertEquals(expected, result)
  }

  @Test
  fun `ActorContext#toFeatureFlagContext happy path destination`() {
    val input =
      ActorContext()
        .withWorkspaceId(workspaceId)
        .withOrganizationId(organizationId)
        .withActorId(destinationId)
        .withActorDefinitionId(destinationDefinitionId)
        .withActorType(ActorType.DESTINATION)

    val result = input.toFeatureFlagContext()

    val expected =
      Multi(
        listOf(
          Workspace(workspaceId),
          Organization(organizationId),
          Destination(destinationId),
          DestinationDefinition(destinationDefinitionId),
        ),
      )

    assertEquals(expected, result)
  }

  @Test
  fun `ActorContext#toFeatureFlagContext empty`() {
    val input = ActorContext()

    val result = input.toFeatureFlagContext()

    val expected = Empty

    assertEquals(expected, result)
  }

  @Test
  fun `ActorContext#toFeatureFlagContext empty source`() {
    val input =
      ActorContext()
        .withActorType(ActorType.SOURCE)

    val result = input.toFeatureFlagContext()

    val expected = Empty

    assertEquals(expected, result)
  }

  @Test
  fun `ActorContext#toFeatureFlagContext empty destination`() {
    val input =
      ActorContext()
        .withActorType(ActorType.DESTINATION)

    val result = input.toFeatureFlagContext()

    val expected = Empty

    assertEquals(expected, result)
  }

  @Test
  fun `IntegrationLauncherConfig#toFeatureFlagContext happy path`() {
    val input =
      IntegrationLauncherConfig()
        .withConnectionId(connectionId)
        .withWorkspaceId(workspaceId)

    val result = input.toFeatureFlagContext()

    val expected =
      Multi(
        listOf(
          Connection(connectionId),
          Workspace(workspaceId),
        ),
      )

    assertEquals(expected, result)
  }

  @Test
  fun `IntegrationLauncherConfig#toFeatureFlagContext empty`() {
    val input = IntegrationLauncherConfig()

    val result = input.toFeatureFlagContext()

    val expected = Empty

    assertEquals(expected, result)
  }

  @Test
  fun `ConnectionContext#toFeatureFlagContext happy path`() {
    val input =
      ConnectionContext()
        .withConnectionId(connectionId)
        .withWorkspaceId(workspaceId)
        .withOrganizationId(organizationId)
        .withDestinationId(destinationId)
        .withDestinationDefinitionId(destinationDefinitionId)
        .withSourceId(sourceId)
        .withSourceDefinitionId(sourceDefinitionId)

    val result = input.toFeatureFlagContext()

    val expected =
      Multi(
        listOf(
          Connection(connectionId),
          Workspace(workspaceId),
          Organization(organizationId),
          Source(sourceId),
          SourceDefinition(sourceDefinitionId),
          Destination(destinationId),
          DestinationDefinition(destinationDefinitionId),
        ),
      )

    assertEquals(expected, result)
  }

  @Test
  fun `ConnectionContext#toFeatureFlagContext empty`() {
    val input = ConnectionContext()

    val result = input.toFeatureFlagContext()

    val expected = Empty

    assertEquals(expected, result)
  }

  @Test
  fun `CheckConnectionInput#toFeatureFlagContext happy path`() {
    val launcherConfig =
      IntegrationLauncherConfig()
        .withConnectionId(connectionId)
        .withWorkspaceId(workspaceId)

    val actorContext =
      ActorContext()
        .withWorkspaceId(workspaceId)
        .withOrganizationId(organizationId)
        .withActorId(destinationId)
        .withActorDefinitionId(destinationDefinitionId)
        .withActorType(ActorType.DESTINATION)

    val input =
      CheckConnectionInput(
        jobRunConfig = JobRunConfig(),
        launcherConfig = launcherConfig,
        checkConnectionInput =
          StandardCheckConnectionInput()
            .withActorContext(actorContext),
      )

    val result = input.toFeatureFlagContext()

    val expected =
      Multi(
        listOf(
          Workspace(workspaceId),
          Organization(organizationId),
          Destination(destinationId),
          DestinationDefinition(destinationDefinitionId),
          Connection(connectionId),
        ),
      )

    assertEquals(expected, result)
  }

  @Test
  fun `CheckConnectionInput#toFeatureFlagContext null actorContext`() {
    val launcherConfig = IntegrationLauncherConfig()
    val launcherFFContext = Multi(listOf(Connection(connectionId), Workspace(workspaceId)))
    mockkStatic(IntegrationLauncherConfig::toFeatureFlagContext)
    every { launcherConfig.toFeatureFlagContext() } returns launcherFFContext

    val input =
      CheckConnectionInput(
        jobRunConfig = JobRunConfig(),
        launcherConfig = launcherConfig,
        checkConnectionInput = StandardCheckConnectionInput(),
      )

    val result = input.toFeatureFlagContext()

    assertEquals(launcherFFContext, result)
  }

  @Test
  fun `DiscoverCatalogInput#toFeatureFlagContext happy path`() {
    val launcherConfig =
      IntegrationLauncherConfig()
        .withConnectionId(connectionId)
        .withWorkspaceId(workspaceId)

    val actorContext =
      ActorContext()
        .withWorkspaceId(workspaceId)
        .withOrganizationId(organizationId)
        .withActorId(destinationId)
        .withActorDefinitionId(destinationDefinitionId)
        .withActorType(ActorType.DESTINATION)

    val input =
      DiscoverCatalogInput(
        jobRunConfig = JobRunConfig(),
        launcherConfig = launcherConfig,
        discoverCatalogInput =
          StandardDiscoverCatalogInput()
            .withActorContext(actorContext),
      )

    val result = input.toFeatureFlagContext()

    val expected =
      Multi(
        listOf(
          Workspace(workspaceId),
          Organization(organizationId),
          Destination(destinationId),
          DestinationDefinition(destinationDefinitionId),
          Connection(connectionId),
        ),
      )

    assertEquals(expected, result)
  }

  @Test
  fun `DiscoverCatalogInput#toFeatureFlagContext null actorContext`() {
    val launcherConfig = IntegrationLauncherConfig()
    val launcherFFContext = Multi(listOf(Connection(connectionId), Workspace(workspaceId)))
    mockkStatic(IntegrationLauncherConfig::toFeatureFlagContext)
    every { launcherConfig.toFeatureFlagContext() } returns launcherFFContext

    val input =
      DiscoverCatalogInput(
        jobRunConfig = JobRunConfig(),
        launcherConfig = launcherConfig,
        discoverCatalogInput = StandardDiscoverCatalogInput(),
      )

    val result = input.toFeatureFlagContext()

    assertEquals(launcherFFContext, result)
  }

  @Test
  fun `SpecInput#toFeatureFlagContext happy path`() {
    val launcherConfig = IntegrationLauncherConfig()
    val launcherFFContext = Multi(listOf(Connection(connectionId), Workspace(workspaceId)))
    mockkStatic(IntegrationLauncherConfig::toFeatureFlagContext)
    every { launcherConfig.toFeatureFlagContext() } returns launcherFFContext

    val input =
      SpecInput(
        jobRunConfig = JobRunConfig(),
        launcherConfig = launcherConfig,
      )

    val result = input.toFeatureFlagContext()

    assertEquals(launcherFFContext, result)
  }

  @Test
  fun `ReplicationInput#toFeatureFlagContext happy path`() {
    val connectionContext = ConnectionContext()
    val connectionFFContext =
      Multi(
        listOf(
          Connection(connectionId),
          Workspace(workspaceId),
          Organization(organizationId),
          Source(sourceId),
          SourceDefinition(sourceDefinitionId),
          Destination(destinationId),
          DestinationDefinition(destinationDefinitionId),
        ),
      )
    mockkStatic(ConnectionContext::toFeatureFlagContext)
    every { connectionContext.toFeatureFlagContext() } returns connectionFFContext

    val input =
      ReplicationInput()
        .withConnectionContext(connectionContext)

    val result = input.toFeatureFlagContext()

    assertEquals(connectionFFContext, result)
  }

  @Test
  fun `ReplicationInput#toFeatureFlagContext null connectionContext`() {
    val input = ReplicationInput()

    val result = input.toFeatureFlagContext()

    val expected = Empty

    assertEquals(expected, result)
  }

  object Fixtures {
    val workspaceId: UUID = UUID.randomUUID()
    val organizationId: UUID = UUID.randomUUID()
    val connectionId: UUID = UUID.randomUUID()
    val sourceId: UUID = UUID.randomUUID()
    val sourceDefinitionId: UUID = UUID.randomUUID()
    val destinationId: UUID = UUID.randomUUID()
    val destinationDefinitionId: UUID = UUID.randomUUID()
  }
}
