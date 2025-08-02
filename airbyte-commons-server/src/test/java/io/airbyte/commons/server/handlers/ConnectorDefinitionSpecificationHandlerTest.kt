/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.api.model.generated.SourceDefinitionIdWithWorkspaceId
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.lang.Exceptions.toRuntime
import io.airbyte.commons.server.converters.JobConverter
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.DestinationConnection
import io.airbyte.config.DestinationOAuthParameter
import io.airbyte.config.SourceConnection
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OAuthService
import io.airbyte.data.services.SourceService
import io.airbyte.domain.models.EntitledConnectorSpec
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.services.entitlements.ConnectorConfigEntitlementService
import io.airbyte.persistence.job.WorkspaceHelper
import io.airbyte.protocol.models.v0.AdvancedAuth
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.protocol.models.v0.DestinationSyncMode
import io.airbyte.protocol.models.v0.OAuthConfigSpecification
import io.airbyte.validation.json.JsonValidationException
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.assertj.core.api.CollectionAssert
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.io.IOException
import java.net.URI
import java.util.List
import java.util.Optional
import java.util.UUID

/**
 * Unit tests for [ConnectorDefinitionSpecificationHandler].
 */
internal class ConnectorDefinitionSpecificationHandlerTest {
  private lateinit var actorDefinitionVersionHelper: ActorDefinitionVersionHelper
  private lateinit var jobConverter: JobConverter
  private lateinit var connectorDefinitionSpecificationHandler: ConnectorDefinitionSpecificationHandler

  private lateinit var sourceService: SourceService
  private lateinit var destinationService: DestinationService
  private lateinit var oAuthService: OAuthService
  private lateinit var workspaceHelper: WorkspaceHelper
  private lateinit var connectorEntitlementSerivce: ConnectorConfigEntitlementService

  @BeforeEach
  fun setup() {
    actorDefinitionVersionHelper = mockk()
    jobConverter = mockk(relaxed = true)
    sourceService = mockk()
    destinationService = mockk()
    oAuthService = mockk()
    workspaceHelper = mockk()
    connectorEntitlementSerivce = mockk()

    every { connectorEntitlementSerivce.getEntitledConnectorSpec(anyNullable(), anyNullable()) } answers {
      val actorDefVersion = args[1] as ActorDefinitionVersion
      EntitledConnectorSpec(actorDefVersion.spec, mutableListOf())
    }

    connectorDefinitionSpecificationHandler =
      ConnectorDefinitionSpecificationHandler(
        actorDefinitionVersionHelper,
        jobConverter,
        sourceService,
        destinationService,
        workspaceHelper,
        oAuthService,
        connectorEntitlementSerivce,
      )
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testGetDestinationSpecForDestinationId() {
    val destinationId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val destinationDefinitionId = UUID.randomUUID()
    val destinationIdRequestBody =
      DestinationIdRequestBody()
        .destinationId(destinationId)

    val destinationDefinition =
      StandardDestinationDefinition()
        .withName(NAME)
        .withDestinationDefinitionId(destinationDefinitionId)
    val destinationVersion =
      ActorDefinitionVersion()
        .withDockerImageTag(DESTINATION_DOCKER_TAG)
        .withSpec(CONNECTOR_SPECIFICATION)
    every { destinationService.getDestinationConnection(destinationId) } returns (
      DestinationConnection()
        .withDestinationId(destinationId)
        .withWorkspaceId(workspaceId)
        .withDestinationDefinitionId(destinationDefinitionId)
    )
    every { destinationService.getStandardDestinationDefinition(destinationDefinitionId) } returns destinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        workspaceId,
        destinationId,
      )
    } returns (destinationVersion)
    every { workspaceHelper.getOrganizationForWorkspace(workspaceId) } returns (organizationId)

    val response =
      connectorDefinitionSpecificationHandler.getSpecificationForDestinationId(destinationIdRequestBody)

    verify { actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId, destinationId) }
    verify { connectorEntitlementSerivce.getEntitledConnectorSpec(OrganizationId(organizationId), destinationVersion) }
    Assertions.assertEquals(CONNECTOR_SPECIFICATION.getConnectionSpecification(), response.getConnectionSpecification())
  }

  @Test
  @Throws(
    JsonValidationException::class,
    IOException::class,
    ConfigNotFoundException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testGetSourceSpecWithoutDocs() {
    val workspaceId = UUID.randomUUID()
    val sourceDefinitionIdWithWorkspaceId =
      SourceDefinitionIdWithWorkspaceId().sourceDefinitionId(UUID.randomUUID()).workspaceId(workspaceId)

    val sourceDefinition =
      StandardSourceDefinition()
        .withName(NAME)
        .withSourceDefinitionId(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId())
    every { sourceService.getStandardSourceDefinition(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId()) } returns sourceDefinition
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        sourceDefinitionIdWithWorkspaceId.getWorkspaceId(),
      )
    } returns
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withSpec(CONNECTOR_SPECIFICATION_WITHOUT_DOCS_URL)

    every { workspaceHelper.getOrganizationForWorkspace(workspaceId) } returns (UUID.randomUUID())

    val response =
      connectorDefinitionSpecificationHandler.getSourceDefinitionSpecification(sourceDefinitionIdWithWorkspaceId)

    verify { sourceService.getStandardSourceDefinition(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId()) }
    verify { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, sourceDefinitionIdWithWorkspaceId.getWorkspaceId()) }
    Assertions.assertEquals(CONNECTOR_SPECIFICATION_WITHOUT_DOCS_URL.getConnectionSpecification(), response.getConnectionSpecification())
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testGetSourceSpecForSourceId() {
    val sourceId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val sourceDefinitionId = UUID.randomUUID()

    val sourceIdRequestBody =
      SourceIdRequestBody()
        .sourceId(sourceId)

    val sourceDefinition =
      StandardSourceDefinition()
        .withName(NAME)
        .withSourceDefinitionId(sourceDefinitionId)
    every { sourceService.getSourceConnection(sourceId) } returns (
      SourceConnection()
        .withSourceId(sourceId)
        .withWorkspaceId(workspaceId)
        .withSourceDefinitionId(sourceDefinitionId)
    )
    every { sourceService.getStandardSourceDefinition(sourceDefinitionId) } returns (sourceDefinition)
    every { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, workspaceId, sourceId) } returns
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withSpec(CONNECTOR_SPECIFICATION)

    val response = connectorDefinitionSpecificationHandler.getSpecificationForSourceId(sourceIdRequestBody)

    verify { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, workspaceId, sourceId) }
    Assertions.assertEquals(CONNECTOR_SPECIFICATION.getConnectionSpecification(), response.getConnectionSpecification())
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testGetDestinationSpec() {
    val destinationDefinitionIdWithWorkspaceId =
      DestinationDefinitionIdWithWorkspaceId().destinationDefinitionId(UUID.randomUUID()).workspaceId(UUID.randomUUID())

    val organizationId = UUID.randomUUID()
    every { workspaceHelper.getOrganizationForWorkspace(destinationDefinitionIdWithWorkspaceId.getWorkspaceId()) } returns organizationId

    val destinationDefinition =
      StandardDestinationDefinition()
        .withName(NAME)
        .withDestinationDefinitionId(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId())
    val destinationVersion =
      ActorDefinitionVersion()
        .withDockerImageTag(DESTINATION_DOCKER_TAG)
        .withSpec(CONNECTOR_SPECIFICATION)
    every { destinationService.getStandardDestinationDefinition(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId()) }
      .returns(destinationDefinition)
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        destinationDefinitionIdWithWorkspaceId.getWorkspaceId(),
      )
    } returns (destinationVersion)

    val response =
      connectorDefinitionSpecificationHandler.getDestinationSpecification(destinationDefinitionIdWithWorkspaceId)

    verify { destinationService.getStandardDestinationDefinition(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId()) }
    verify { actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, destinationDefinitionIdWithWorkspaceId.getWorkspaceId()) }
    verify { connectorEntitlementSerivce.getEntitledConnectorSpec(OrganizationId(organizationId), destinationVersion) }
    Assertions.assertEquals(CONNECTOR_SPECIFICATION.getConnectionSpecification(), response.getConnectionSpecification())
  }

  @Test
  @Throws(
    JsonValidationException::class,
    IOException::class,
    ConfigNotFoundException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testGetSourceSpec() {
    val workspaceId = UUID.randomUUID()
    val sourceDefinitionIdWithWorkspaceId =
      SourceDefinitionIdWithWorkspaceId().sourceDefinitionId(UUID.randomUUID()).workspaceId(workspaceId)

    val sourceDefinition =
      StandardSourceDefinition()
        .withName(NAME)
        .withSourceDefinitionId(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId())
    every { sourceService.getStandardSourceDefinition(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId()) } returns sourceDefinition
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        sourceDefinitionIdWithWorkspaceId.getWorkspaceId(),
      )
    } returns (
      ActorDefinitionVersion()
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withSpec(CONNECTOR_SPECIFICATION)
    )
    every { workspaceHelper.getOrganizationForWorkspace(workspaceId) } returns (UUID.randomUUID())

    val response =
      connectorDefinitionSpecificationHandler.getSourceDefinitionSpecification(sourceDefinitionIdWithWorkspaceId)

    verify { sourceService.getStandardSourceDefinition(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId()) }
    verify { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, sourceDefinitionIdWithWorkspaceId.getWorkspaceId()) }
    Assertions.assertEquals(CONNECTOR_SPECIFICATION.getConnectionSpecification(), response.getConnectionSpecification())
  }

  @ValueSource(booleans = [true, false])
  @ParameterizedTest
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testDestinationSyncModeEnrichment(supportsRefreshes: Boolean) {
    val workspaceId = UUID.randomUUID()
    val destinationDefinitionIdWithWorkspaceId =
      DestinationDefinitionIdWithWorkspaceId().destinationDefinitionId(UUID.randomUUID()).workspaceId(workspaceId)

    val destinationDefinition =
      StandardDestinationDefinition()
        .withName(NAME)
        .withDestinationDefinitionId(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId())
    every {
      destinationService.getStandardDestinationDefinition(
        destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId(),
      )
    } returns (destinationDefinition)
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        destinationDefinitionIdWithWorkspaceId.getWorkspaceId(),
      )
    } returns (
      ActorDefinitionVersion()
        .withDockerImageTag(DESTINATION_DOCKER_TAG)
        .withSpec(
          ConnectorSpecification()
            .withDocumentationUrl(toRuntime<URI?> { URI(CONNECTOR_URL) })
            .withChangelogUrl(toRuntime<URI?> { URI(CONNECTOR_URL) })
            .withConnectionSpecification(jsonNode<HashMap<Any?, Any?>?>(HashMap<Any?, Any?>()))
            .withSupportedDestinationSyncModes(
              List.of<DestinationSyncMode?>(
                DestinationSyncMode.APPEND,
                DestinationSyncMode.APPEND_DEDUP,
                DestinationSyncMode.OVERWRITE,
                DestinationSyncMode.UPDATE,
                DestinationSyncMode.SOFT_DELETE,
              ),
            ),
        ).withSupportsRefreshes(supportsRefreshes)
    )
    every { workspaceHelper.getOrganizationForWorkspace(workspaceId) } returns (UUID.randomUUID())

    val response =
      connectorDefinitionSpecificationHandler.getDestinationSpecification(destinationDefinitionIdWithWorkspaceId)

    verify { destinationService.getStandardDestinationDefinition(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId()) }
    verify { actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, destinationDefinitionIdWithWorkspaceId.getWorkspaceId()) }
    if (supportsRefreshes) {
      CollectionAssert
        .assertThatCollection<io.airbyte.api.model.generated.DestinationSyncMode?>(response.getSupportedDestinationSyncModes())
        .containsExactlyInAnyOrderElementsOf(
          List.of<io.airbyte.api.model.generated.DestinationSyncMode?>(
            io.airbyte.api.model.generated.DestinationSyncMode.APPEND,
            io.airbyte.api.model.generated.DestinationSyncMode.APPEND_DEDUP,
            io.airbyte.api.model.generated.DestinationSyncMode.OVERWRITE,
            io.airbyte.api.model.generated.DestinationSyncMode.SOFT_DELETE,
            io.airbyte.api.model.generated.DestinationSyncMode.UPDATE,
            io.airbyte.api.model.generated.DestinationSyncMode.OVERWRITE_DEDUP,
          ),
        )
    } else {
      CollectionAssert
        .assertThatCollection<io.airbyte.api.model.generated.DestinationSyncMode?>(response.getSupportedDestinationSyncModes())
        .containsExactlyInAnyOrderElementsOf(
          List.of<io.airbyte.api.model.generated.DestinationSyncMode?>(
            io.airbyte.api.model.generated.DestinationSyncMode.APPEND,
            io.airbyte.api.model.generated.DestinationSyncMode.APPEND_DEDUP,
            io.airbyte.api.model.generated.DestinationSyncMode.OVERWRITE,
            io.airbyte.api.model.generated.DestinationSyncMode.UPDATE,
            io.airbyte.api.model.generated.DestinationSyncMode.SOFT_DELETE,
          ),
        )
    }
  }

  @ValueSource(booleans = [true, false])
  @ParameterizedTest
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testDestinationSyncModeEnrichmentWithoutOverwrite(supportsRefreshes: Boolean) {
    val workspaceId = UUID.randomUUID()
    val destinationDefinitionIdWithWorkspaceId =
      DestinationDefinitionIdWithWorkspaceId().destinationDefinitionId(UUID.randomUUID()).workspaceId(workspaceId)

    val destinationDefinition =
      StandardDestinationDefinition()
        .withName(NAME)
        .withDestinationDefinitionId(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId())
    every {
      destinationService.getStandardDestinationDefinition(
        destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId(),
      )
    } returns (destinationDefinition)
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        destinationDefinitionIdWithWorkspaceId.getWorkspaceId(),
      )
    } returns (
      ActorDefinitionVersion()
        .withDockerImageTag(DESTINATION_DOCKER_TAG)
        .withSpec(
          ConnectorSpecification()
            .withDocumentationUrl(toRuntime<URI?> { URI(CONNECTOR_URL) })
            .withChangelogUrl(toRuntime<URI?> { URI(CONNECTOR_URL) })
            .withConnectionSpecification(jsonNode<HashMap<Any?, Any?>?>(HashMap<Any?, Any?>()))
            .withSupportedDestinationSyncModes(
              List.of<DestinationSyncMode?>(
                DestinationSyncMode.APPEND,
                DestinationSyncMode.APPEND_DEDUP,
              ),
            ),
        ).withSupportsRefreshes(supportsRefreshes)
    )
    every { workspaceHelper.getOrganizationForWorkspace(workspaceId) } returns (UUID.randomUUID())

    val response =
      connectorDefinitionSpecificationHandler.getDestinationSpecification(destinationDefinitionIdWithWorkspaceId)

    verify { destinationService.getStandardDestinationDefinition(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId()) }
    verify { actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, destinationDefinitionIdWithWorkspaceId.getWorkspaceId()) }
    CollectionAssert
      .assertThatCollection<io.airbyte.api.model.generated.DestinationSyncMode?>(response.getSupportedDestinationSyncModes())
      .containsExactlyInAnyOrderElementsOf(
        List.of<io.airbyte.api.model.generated.DestinationSyncMode?>(
          io.airbyte.api.model.generated.DestinationSyncMode.APPEND,
          io.airbyte.api.model.generated.DestinationSyncMode.APPEND_DEDUP,
        ),
      )
  }

  @ValueSource(booleans = [true, false])
  @ParameterizedTest
  @Throws(IOException::class)
  fun getDestinationSpecificationReadAdvancedAuth(advancedAuthGlobalCredentialsAvailable: Boolean) {
    val workspaceId = UUID.randomUUID()
    val destinationDefinitionId = UUID.randomUUID()
    every {
      oAuthService.getDestinationOAuthParameterOptional(
        workspaceId,
        destinationDefinitionId,
      )
    } returns (if (advancedAuthGlobalCredentialsAvailable) Optional.of(DestinationOAuthParameter()) else Optional.empty())

    val destinationDefinitionIdWithWorkspaceId =
      DestinationDefinitionIdWithWorkspaceId().destinationDefinitionId(destinationDefinitionId).workspaceId(workspaceId)
    val destinationDefinition =
      StandardDestinationDefinition()
        .withName(NAME)
        .withDestinationDefinitionId(destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId())

    val connectorSpecification =
      ConnectorSpecification()
        .withDocumentationUrl(toRuntime<URI?> { URI(CONNECTOR_URL) })
        .withChangelogUrl(toRuntime<URI?> { URI(CONNECTOR_URL) })
        .withConnectionSpecification(jsonNode<HashMap<Any?, Any?>?>(HashMap()))
        .withAdvancedAuth(
          AdvancedAuth().withAuthFlowType(AdvancedAuth.AuthFlowType.OAUTH_2_0).withOauthConfigSpecification(OAuthConfigSpecification()),
        )

    val entitledConnectorSpec = EntitledConnectorSpec(connectorSpecification, mutableListOf())

    val response =
      connectorDefinitionSpecificationHandler.getDestinationSpecificationRead(destinationDefinition, entitledConnectorSpec, true, workspaceId)

    verify { oAuthService.getDestinationOAuthParameterOptional(workspaceId, destinationDefinitionId) }
    Assertions.assertEquals(advancedAuthGlobalCredentialsAvailable, response.getAdvancedAuthGlobalCredentialsAvailable())
  }

  @ValueSource(booleans = [true, false])
  @ParameterizedTest
  @Throws(IOException::class)
  fun getSourceSpecificationReadAdvancedAuth(advancedAuthGlobalCredentialsAvailable: Boolean) {
    val workspaceId = UUID.randomUUID()
    val sourceDefinitionId = UUID.randomUUID()
    every { oAuthService.getSourceOAuthParameterOptional(workspaceId, sourceDefinitionId) } returns
      (if (advancedAuthGlobalCredentialsAvailable) Optional.of(SourceOAuthParameter()) else Optional.empty())

    val sourceDefinitionIdWithWorkspaceId =
      SourceDefinitionIdWithWorkspaceId().sourceDefinitionId(sourceDefinitionId).workspaceId(workspaceId)
    val sourceDefinition =
      StandardSourceDefinition()
        .withName(NAME)
        .withSourceDefinitionId(sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId())

    val connectorSpecification =
      ConnectorSpecification()
        .withDocumentationUrl(toRuntime<URI?> { URI(CONNECTOR_URL) })
        .withChangelogUrl(toRuntime<URI?> { URI(CONNECTOR_URL) })
        .withConnectionSpecification(jsonNode<HashMap<Any?, Any?>?>(HashMap()))
        .withAdvancedAuth(
          AdvancedAuth().withAuthFlowType(AdvancedAuth.AuthFlowType.OAUTH_2_0).withOauthConfigSpecification(OAuthConfigSpecification()),
        )

    val entitledConnectorSpec = EntitledConnectorSpec(connectorSpecification, mutableListOf())

    val response =
      connectorDefinitionSpecificationHandler.getSourceSpecificationRead(sourceDefinition, entitledConnectorSpec, workspaceId)

    verify { oAuthService.getSourceOAuthParameterOptional(workspaceId, sourceDefinitionId) }
    Assertions.assertEquals(advancedAuthGlobalCredentialsAvailable, response.getAdvancedAuthGlobalCredentialsAvailable())
  }

  companion object {
    private const val CONNECTOR_URL = "https://google.com"
    private const val DESTINATION_DOCKER_TAG = "tag"
    private const val NAME = "name"
    private const val SOURCE_DOCKER_REPO = "srcimage"
    private const val SOURCE_DOCKER_TAG = "tag"

    private val CONNECTOR_SPECIFICATION: ConnectorSpecification =
      ConnectorSpecification()
        .withDocumentationUrl(toRuntime<URI?> { URI(CONNECTOR_URL) })
        .withChangelogUrl(toRuntime<URI?> { URI(CONNECTOR_URL) })
        .withConnectionSpecification(jsonNode<HashMap<Any?, Any?>?>(HashMap<Any?, Any?>()))

    private val CONNECTOR_SPECIFICATION_WITHOUT_DOCS_URL: ConnectorSpecification =
      ConnectorSpecification()
        .withChangelogUrl(toRuntime<URI?> { URI(CONNECTOR_URL) })
        .withConnectionSpecification(jsonNode<HashMap<Any?, Any?>?>(HashMap<Any?, Any?>()))
  }
}
