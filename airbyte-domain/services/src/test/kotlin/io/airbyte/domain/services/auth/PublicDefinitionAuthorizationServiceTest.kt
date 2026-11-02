/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.commons.constants.AirbyteCatalogConstants
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.shared.ConnectorBuilderProjectMetadata
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.Optional
import java.util.UUID

class PublicDefinitionAuthorizationServiceTest {
  private val sources = mockk<SourceService>()
  private val destinations = mockk<DestinationService>()
  private val projects = mockk<ConnectorBuilderService>()
  private val actors = mockk<ActorDefinitionService>()
  private val service = PublicDefinitionAuthorizationService(sources, destinations, projects, actors)
  private val definition = UUID(0, 1)
  private val project = UUID(0, 2)
  private val workspace = UUID(0, 3)

  @Test
  fun `definition metadata uses its saved kind custom marker and default image`() {
    every { sources.getStandardSourceDefinition(definition) } returns
      StandardSourceDefinition().withSourceDefinitionId(definition).withCustom(true).withTombstone(false)
    every { actors.getDefaultVersionForActorDefinitionIdOptional(definition) } returns
      Optional.of(
        ActorDefinitionVersion()
          .withActorDefinitionId(
            definition,
          ).withDockerRepository(AirbyteCatalogConstants.AIRBYTE_SOURCE_DECLARATIVE_MANIFEST_IMAGE),
      )
    assertEquals(PublicDefinitionMetadata(true, true), service.definition(definition, true))
    every { destinations.getStandardDestinationDefinition(definition) } returns
      StandardDestinationDefinition().withDestinationDefinitionId(definition).withCustom(false).withTombstone(false)
    assertEquals(PublicDefinitionMetadata(false, false), service.definition(definition, false))
  }

  @Test
  fun `missing deleted mismatched and failed metadata cannot establish a definition`() {
    every { sources.getStandardSourceDefinition(definition) } returns
      StandardSourceDefinition().withSourceDefinitionId(definition).withTombstone(true)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.definition(definition, true) }
    every { sources.getStandardSourceDefinition(definition) } returns
      StandardSourceDefinition().withSourceDefinitionId(definition).withTombstone(false)
    every { actors.getDefaultVersionForActorDefinitionIdOptional(definition) } returns Optional.empty()
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.definition(definition, true) }
    every { actors.getDefaultVersionForActorDefinitionIdOptional(definition) } returns
      Optional.of(ActorDefinitionVersion().withActorDefinitionId(UUID(0, 9)))
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.definition(definition, true) }
    val failure = IOException("metadata unavailable")
    every { sources.getStandardSourceDefinition(definition) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.definition(definition, true) })
  }

  @Test
  fun `declarative project ownership requires the exact saved definition and workspace association`() {
    every { projects.getConnectorBuilderProjectIdForActorDefinitionId(definition) } returns Optional.of(project)
    every { projects.getConnectorBuilderProjectMetadata(project) } returns
      ConnectorBuilderProjectMetadata(project, Optional.of(workspace), Optional.of(definition))
    assertEquals(workspace, service.builderWorkspace(definition))
    every { projects.getConnectorBuilderProjectMetadata(project) } returns
      ConnectorBuilderProjectMetadata(project, Optional.of(workspace), Optional.of(UUID(0, 9)))
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.builderWorkspace(definition) }
    every { projects.getConnectorBuilderProjectMetadata(project) } returns
      ConnectorBuilderProjectMetadata(project, Optional.empty(), Optional.of(definition))
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.builderWorkspace(definition) }
    every { projects.getConnectorBuilderProjectIdForActorDefinitionId(definition) } returns Optional.empty()
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.builderWorkspace(definition) }
  }
}
