/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.helpers.ContextBuilder
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorContext
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.AllowedHosts
import io.airbyte.config.DestinationConnection
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.ScopedResourceRequirements
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.ConfigInjector
import io.airbyte.data.repositories.ActorDefinitionRepository
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.repositories.entities.ActorDefinition
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SourceService
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.airbyte.domain.services.secrets.SecretReferenceService
import io.airbyte.persistence.job.factory.OAuthConfigSupplier
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.workers.models.CheckConnectionInput
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class JobInputServiceTest {
  private lateinit var sourceService: SourceService
  private lateinit var destinationService: DestinationService
  private lateinit var actorDefinitionVersionHelper: ActorDefinitionVersionHelper
  private lateinit var actorRepository: ActorRepository
  private lateinit var actorDefinitionRepository: ActorDefinitionRepository
  private lateinit var oAuthConfigSupplier: OAuthConfigSupplier
  private lateinit var configInjector: ConfigInjector
  private lateinit var secretReferenceService: SecretReferenceService
  private lateinit var contextBuilder: ContextBuilder
  private lateinit var scopedConfigurationService: ScopedConfigurationService
  private lateinit var jobInputService: JobInputService

  private val workspaceId = UUID.randomUUID()
  private val sourceDefinitionId = UUID.randomUUID()
  private val sourceId = UUID.randomUUID()
  private val destinationDefinitionId = UUID.randomUUID()
  private val destinationId = UUID.randomUUID()
  private val configuration: JsonNode = Jsons.jsonNode(mapOf("test" to "value"))
  private val emptyAllowedHosts = AllowedHosts()
  private val dockerRepository = "airbyte/docker"
  private val dockerImageTag = "1.0.0"
  private val testDockerImage = "$dockerRepository:$dockerImageTag"

  @BeforeEach
  fun setup() {
    sourceService = mockk()
    destinationService = mockk()
    actorDefinitionVersionHelper = mockk()
    actorRepository = mockk()
    actorDefinitionRepository = mockk()
    oAuthConfigSupplier = mockk()
    configInjector = mockk()
    secretReferenceService = mockk()
    contextBuilder = mockk()
    scopedConfigurationService = mockk()

    jobInputService =
      JobInputService(
        sourceService,
        destinationService,
        actorDefinitionVersionHelper,
        actorRepository,
        actorDefinitionRepository,
        oAuthConfigSupplier,
        configInjector,
        secretReferenceService,
        contextBuilder,
        scopedConfigurationService,
      )
  }

  @Test
  fun `getCheckInput for Source by actorId returns CheckConnectionInput`() {
    val mockActor =
      mockk<io.airbyte.data.repositories.entities.Actor> {
        every { actorType } returns ActorType.source
      }

    val mockSource = mockk<SourceConnection>()
    every { mockSource.sourceId } returns sourceId
    every { mockSource.sourceDefinitionId } returns sourceDefinitionId
    every { mockSource.workspaceId } returns workspaceId
    every { mockSource.configuration } returns configuration

    val mockSourceDefinition = mockk<StandardSourceDefinition>()
    every { mockSourceDefinition.sourceDefinitionId } returns sourceDefinitionId
    every { mockSourceDefinition.custom } returns false
    every { mockSourceDefinition.resourceRequirements } returns null

    val mockActorDefinitionVersion = mockk<ActorDefinitionVersion>()
    every { mockActorDefinitionVersion.dockerImageTag } returns dockerImageTag
    every { mockActorDefinitionVersion.dockerRepository } returns dockerRepository
    every { mockActorDefinitionVersion.protocolVersion } returns "0.1.0"
    every { mockActorDefinitionVersion.allowedHosts } returns emptyAllowedHosts

    every { actorRepository.findByActorId(sourceId) } returns mockActor
    every { sourceService.getSourceConnection(sourceId) } returns mockSource
    every { sourceService.getStandardSourceDefinition(sourceDefinitionId) } returns mockSourceDefinition
    every { actorDefinitionVersionHelper.getSourceVersion(mockSourceDefinition, workspaceId, sourceId) } returns mockActorDefinitionVersion
    every { oAuthConfigSupplier.injectSourceOAuthParameters(any(), any(), any(), any()) } returns configuration
    every { configInjector.injectConfig(any(), any()) } returns configuration
    every { secretReferenceService.getConfigWithSecretReferences(any(), any(), any()) } returns mockk { every { config } returns configuration }
    every { contextBuilder.fromSource(any()) } returns mockk()
    every { scopedConfigurationService.getScopedConfigurations(any(), any()) } returns emptyList()

    val jobId = "jobid"
    val attemptId = 1337L

    val expected =
      CheckConnectionInput(
        jobRunConfig = JobRunConfig().withJobId(jobId).withAttemptId(attemptId),
        launcherConfig =
          IntegrationLauncherConfig()
            .withJobId(jobId)
            .withWorkspaceId(workspaceId)
            .withDockerImage(testDockerImage)
            .withProtocolVersion(Version("0.1.0"))
            .withIsCustomConnector(false)
            .withAttemptId(0L)
            .withAllowedHosts(emptyAllowedHosts),
        checkConnectionInput =
          StandardCheckConnectionInput()
            .withActorType(io.airbyte.config.ActorType.SOURCE)
            .withActorId(sourceId)
            .withConnectionConfiguration(configuration)
            .withResourceRequirements(ResourceRequirements())
            .withActorContext(mockk())
            .withNetworkSecurityTokens(emptyList()),
      )

    val actual = jobInputService.getCheckInput(sourceId, jobId, attemptId)

    assertEquals(expected.launcherConfig.jobId, actual.launcherConfig.jobId)
    assertEquals(expected.jobRunConfig.jobId, actual.jobRunConfig.jobId)
    assertEquals(expected.jobRunConfig.attemptId, actual.jobRunConfig.attemptId)

    assertEquals(expected.checkConnectionInput.actorType, actual.checkConnectionInput.actorType)
    assertEquals(expected.checkConnectionInput.actorId, actual.checkConnectionInput.actorId)
    assertEquals(expected.checkConnectionInput.connectionConfiguration, actual.checkConnectionInput.connectionConfiguration)

    assertEquals(expected.launcherConfig.workspaceId, actual.launcherConfig.workspaceId)
    assertEquals(expected.launcherConfig.dockerImage, actual.launcherConfig.dockerImage)
    assertEquals(expected.launcherConfig.protocolVersion, actual.launcherConfig.protocolVersion)
    assertEquals(expected.launcherConfig.isCustomConnector, actual.launcherConfig.isCustomConnector)
  }

  @Test
  fun `getCheckInput for Destination by actorId returns CheckConnectionInput`() {
    val mockActor =
      mockk<io.airbyte.data.repositories.entities.Actor> {
        every { actorType } returns ActorType.destination
      }

    val mockDestination = mockk<DestinationConnection>()
    every { mockDestination.destinationId } returns destinationId
    every { mockDestination.destinationDefinitionId } returns destinationDefinitionId
    every { mockDestination.workspaceId } returns workspaceId
    every { mockDestination.configuration } returns configuration

    val mockDestinationDefinition = mockk<StandardDestinationDefinition>()
    every { mockDestinationDefinition.destinationDefinitionId } returns destinationDefinitionId
    every { mockDestinationDefinition.custom } returns true
    every { mockDestinationDefinition.resourceRequirements } returns ScopedResourceRequirements()

    val mockActorDefinitionVersion = mockk<ActorDefinitionVersion>()
    every { mockActorDefinitionVersion.dockerRepository } returns dockerRepository
    every { mockActorDefinitionVersion.dockerImageTag } returns dockerImageTag
    every { mockActorDefinitionVersion.protocolVersion } returns "0.2.0"
    every { mockActorDefinitionVersion.allowedHosts } returns emptyAllowedHosts

    every { actorRepository.findByActorId(destinationId) } returns mockActor
    every { destinationService.getDestinationConnection(destinationId) } returns mockDestination
    every { destinationService.getStandardDestinationDefinition(destinationDefinitionId) } returns mockDestinationDefinition
    every { actorDefinitionVersionHelper.getDestinationVersion(mockDestinationDefinition, workspaceId, destinationId) } returns
      mockActorDefinitionVersion
    every { oAuthConfigSupplier.injectDestinationOAuthParameters(any(), any(), any(), any()) } returns configuration
    every { configInjector.injectConfig(any(), any()) } returns configuration
    every { secretReferenceService.getConfigWithSecretReferences(any(), any(), any()) } returns mockk { every { config } returns configuration }
    every { contextBuilder.fromDestination(any()) } returns mockk()
    every { scopedConfigurationService.getScopedConfigurations(any(), any()) } returns emptyList()

    val jobId = "jobid"
    val attemptId = 1337L

    val expected =
      CheckConnectionInput(
        jobRunConfig = JobRunConfig().withJobId(jobId).withAttemptId(attemptId),
        launcherConfig =
          IntegrationLauncherConfig()
            .withJobId(jobId)
            .withWorkspaceId(workspaceId)
            .withDockerImage(testDockerImage)
            .withProtocolVersion(Version("0.2.0"))
            .withIsCustomConnector(true)
            .withAttemptId(attemptId)
            .withAllowedHosts(emptyAllowedHosts),
        checkConnectionInput =
          StandardCheckConnectionInput()
            .withActorType(io.airbyte.config.ActorType.DESTINATION)
            .withActorId(destinationId)
            .withConnectionConfiguration(configuration)
            .withResourceRequirements(ResourceRequirements())
            .withActorContext(mockk())
            .withNetworkSecurityTokens(emptyList()),
      )

    val actual = jobInputService.getCheckInput(destinationId, jobId, attemptId)

    assertEquals(expected.launcherConfig.jobId, actual.launcherConfig.jobId)
    assertEquals(expected.jobRunConfig.jobId, actual.jobRunConfig.jobId)
    assertEquals(expected.jobRunConfig.attemptId, actual.jobRunConfig.attemptId)

    assertEquals(expected.checkConnectionInput.actorType, actual.checkConnectionInput.actorType)
    assertEquals(expected.checkConnectionInput.actorId, actual.checkConnectionInput.actorId)
    assertEquals(expected.checkConnectionInput.connectionConfiguration, actual.checkConnectionInput.connectionConfiguration)

    assertEquals(expected.launcherConfig.workspaceId, actual.launcherConfig.workspaceId)
    assertEquals(expected.launcherConfig.dockerImage, actual.launcherConfig.dockerImage)
    assertEquals(expected.launcherConfig.protocolVersion, actual.launcherConfig.protocolVersion)
    assertEquals(expected.launcherConfig.isCustomConnector, actual.launcherConfig.isCustomConnector)
    assertEquals(expected.launcherConfig.jobId, actual.launcherConfig.jobId)
    assertEquals(expected.launcherConfig.attemptId, actual.launcherConfig.attemptId)
  }

  @Test
  fun `getCheckInput for Source by definitionId and workspaceId returns CheckConnectionInput`() {
    val mockActorDefinition =
      mockk<ActorDefinition> {
        every { actorType } returns ActorType.source
      }

    val mockSourceDefinition = mockk<StandardSourceDefinition>()
    every { mockSourceDefinition.sourceDefinitionId } returns sourceDefinitionId
    every { mockSourceDefinition.custom } returns false
    every { mockSourceDefinition.resourceRequirements } returns null

    val mockActorDefinitionVersion = mockk<ActorDefinitionVersion>()
    every { mockActorDefinitionVersion.dockerImageTag } returns dockerImageTag
    every { mockActorDefinitionVersion.dockerRepository } returns dockerRepository
    every { mockActorDefinitionVersion.protocolVersion } returns "0.1.0"
    every { mockActorDefinitionVersion.allowedHosts } returns emptyAllowedHosts
    every { mockActorDefinitionVersion.spec } returns ConnectorSpecification()

    every { actorDefinitionRepository.findByActorDefinitionId(sourceDefinitionId) } returns mockActorDefinition
    every { sourceService.getStandardSourceDefinition(sourceDefinitionId) } returns mockSourceDefinition
    every { actorDefinitionVersionHelper.getSourceVersion(mockSourceDefinition, workspaceId, null) } returns mockActorDefinitionVersion
    every { oAuthConfigSupplier.maskSourceOAuthParameters(any(), any(), any(), any()) } returns configuration
    every { configInjector.injectConfig(any(), any()) } returns configuration
    every { secretReferenceService.getConfigWithSecretReferences(any(), any(), any()) } returns mockk { every { config } returns configuration }
    every { scopedConfigurationService.getScopedConfigurations(any(), any()) } returns emptyList()

    val actorContext: ActorContext = mockk()
    every { contextBuilder.fromActorDefinitionId(any(), any(), any()) } returns actorContext

    val jobId = UUID.randomUUID().toString()

    val expected =
      CheckConnectionInput(
        jobRunConfig = JobRunConfig().withJobId(jobId).withAttemptId(0L),
        launcherConfig =
          IntegrationLauncherConfig()
            .withJobId(jobId)
            .withWorkspaceId(workspaceId)
            .withDockerImage(testDockerImage)
            .withProtocolVersion(Version("0.1.0"))
            .withIsCustomConnector(false)
            .withAttemptId(0L)
            .withAllowedHosts(emptyAllowedHosts),
        checkConnectionInput =
          StandardCheckConnectionInput()
            .withActorType(io.airbyte.config.ActorType.SOURCE)
            .withActorId(null)
            .withConnectionConfiguration(configuration)
            .withResourceRequirements(null)
            .withActorContext(actorContext)
            .withNetworkSecurityTokens(emptyList()),
      )

    val actual = jobInputService.getCheckInput(sourceDefinitionId, workspaceId, configuration)

    assertEquals(expected.checkConnectionInput.actorType, actual.checkConnectionInput.actorType)
    assertEquals(expected.checkConnectionInput.actorId, actual.checkConnectionInput.actorId)
    assertEquals(expected.checkConnectionInput.connectionConfiguration, actual.checkConnectionInput.connectionConfiguration)
    assertEquals(expected.checkConnectionInput.resourceRequirements, actual.checkConnectionInput.resourceRequirements)
    assertEquals(expected.checkConnectionInput.actorContext, actual.checkConnectionInput.actorContext)
    assertEquals(expected.checkConnectionInput.networkSecurityTokens, actual.checkConnectionInput.networkSecurityTokens)

    assertEquals(expected.launcherConfig.workspaceId, actual.launcherConfig.workspaceId)
    assertEquals(expected.launcherConfig.dockerImage, actual.launcherConfig.dockerImage)
    assertEquals(expected.launcherConfig.protocolVersion, actual.launcherConfig.protocolVersion)
    assertEquals(expected.launcherConfig.isCustomConnector, actual.launcherConfig.isCustomConnector)
  }

  @Test
  fun `getCheckInput for Destination by definitionId and workspaceId returns CheckConnectionInput`() {
    val mockActorDefinition =
      mockk<ActorDefinition> {
        every { actorType } returns ActorType.destination
      }

    val mockDestinationDefinition = mockk<StandardDestinationDefinition>()
    every { mockDestinationDefinition.destinationDefinitionId } returns destinationDefinitionId
    every { mockDestinationDefinition.custom } returns true
    every { mockDestinationDefinition.resourceRequirements } returns ScopedResourceRequirements()

    val mockActorDefinitionVersion = mockk<ActorDefinitionVersion>()
    every { mockActorDefinitionVersion.dockerImageTag } returns dockerImageTag
    every { mockActorDefinitionVersion.dockerRepository } returns dockerRepository
    every { mockActorDefinitionVersion.protocolVersion } returns "0.2.0"
    every { mockActorDefinitionVersion.allowedHosts } returns emptyAllowedHosts
    every { mockActorDefinitionVersion.spec } returns ConnectorSpecification()

    every { actorDefinitionRepository.findByActorDefinitionId(destinationDefinitionId) } returns mockActorDefinition
    every { destinationService.getStandardDestinationDefinition(destinationDefinitionId) } returns mockDestinationDefinition
    every { actorDefinitionVersionHelper.getDestinationVersion(mockDestinationDefinition, workspaceId, null) } returns mockActorDefinitionVersion
    every { oAuthConfigSupplier.maskDestinationOAuthParameters(any(), any(), any(), any()) } returns configuration
    every { configInjector.injectConfig(any(), any()) } returns configuration
    every { secretReferenceService.getConfigWithSecretReferences(any(), any(), any()) } returns mockk { every { config } returns configuration }
    every { contextBuilder.fromActorDefinitionId(any(), any(), any()) } returns mockk()
    every { scopedConfigurationService.getScopedConfigurations(any(), any()) } returns emptyList()

    val jobId = UUID.randomUUID().toString()

    val expected =
      CheckConnectionInput(
        jobRunConfig = JobRunConfig().withJobId(jobId).withAttemptId(0L),
        launcherConfig =
          IntegrationLauncherConfig()
            .withJobId(jobId)
            .withWorkspaceId(workspaceId)
            .withDockerImage(testDockerImage)
            .withProtocolVersion(Version("0.2.0"))
            .withIsCustomConnector(true)
            .withAttemptId(0L)
            .withAllowedHosts(emptyAllowedHosts),
        checkConnectionInput =
          StandardCheckConnectionInput()
            .withActorType(io.airbyte.config.ActorType.DESTINATION)
            .withActorId(null)
            .withConnectionConfiguration(configuration)
            .withResourceRequirements(ResourceRequirements())
            .withActorContext(null)
            .withNetworkSecurityTokens(emptyList()),
      )

    val actual = jobInputService.getCheckInput(destinationDefinitionId, workspaceId, configuration)

    assertEquals(expected.checkConnectionInput.actorType, actual.checkConnectionInput.actorType)
    assertEquals(expected.checkConnectionInput.actorId, actual.checkConnectionInput.actorId)
    assertEquals(expected.checkConnectionInput.connectionConfiguration, actual.checkConnectionInput.connectionConfiguration)

    assertEquals(expected.launcherConfig.workspaceId, actual.launcherConfig.workspaceId)
    assertEquals(expected.launcherConfig.dockerImage, actual.launcherConfig.dockerImage)
    assertEquals(expected.launcherConfig.protocolVersion, actual.launcherConfig.protocolVersion)
    assertEquals(expected.launcherConfig.isCustomConnector, actual.launcherConfig.isCustomConnector)
  }

  @Test
  fun `getDiscoveryInput for Source by actorId returns DiscoverCatalogInput`() {
    val mockActor =
      mockk<io.airbyte.data.repositories.entities.Actor> {
        every { actorType } returns ActorType.source
      }

    val mockSource = mockk<SourceConnection>()
    every { mockSource.sourceId } returns sourceId
    every { mockSource.sourceDefinitionId } returns sourceDefinitionId
    every { mockSource.workspaceId } returns workspaceId
    every { mockSource.configuration } returns configuration

    val mockSourceDefinition = mockk<StandardSourceDefinition>()
    every { mockSourceDefinition.sourceDefinitionId } returns sourceDefinitionId
    every { mockSourceDefinition.custom } returns false
    every { mockSourceDefinition.resourceRequirements } returns null

    val mockActorDefinitionVersion = mockk<ActorDefinitionVersion>()
    every { mockActorDefinitionVersion.dockerImageTag } returns dockerImageTag
    every { mockActorDefinitionVersion.dockerRepository } returns dockerRepository
    every { mockActorDefinitionVersion.protocolVersion } returns "0.1.0"
    every { mockActorDefinitionVersion.allowedHosts } returns emptyAllowedHosts
    every { mockActorDefinitionVersion.spec } returns ConnectorSpecification()

    every { actorRepository.findByActorId(sourceId) } returns mockActor
    every { sourceService.getSourceConnection(sourceId) } returns mockSource
    every { sourceService.getStandardSourceDefinition(sourceDefinitionId) } returns mockSourceDefinition
    every { actorDefinitionVersionHelper.getSourceVersion(mockSourceDefinition, workspaceId, sourceId) } returns mockActorDefinitionVersion
    every { oAuthConfigSupplier.injectSourceOAuthParameters(any(), any(), any(), any()) } returns configuration
    every { configInjector.injectConfig(any(), any()) } returns configuration
    every { secretReferenceService.getConfigWithSecretReferences(any(), any(), any()) } returns mockk { every { config } returns configuration }
    every { contextBuilder.fromSource(any()) } returns mockk()
    every { scopedConfigurationService.getScopedConfigurations(any(), any()) } returns emptyList()

    val actual = jobInputService.getDiscoveryInput(sourceId, workspaceId)

    assertEquals(0L, actual.jobRunConfig.attemptId)

    assertEquals(sourceId.toString(), actual.discoverCatalogInput.sourceId)
    assertEquals(dockerImageTag, actual.discoverCatalogInput.connectorVersion)
    assertEquals(configuration, actual.discoverCatalogInput.connectionConfiguration)
    assertEquals(null, actual.discoverCatalogInput.resourceRequirements)
    assertEquals(true, actual.discoverCatalogInput.manual)

    assertEquals(workspaceId, actual.integrationLauncherConfig.workspaceId)
    assertEquals(testDockerImage, actual.integrationLauncherConfig.dockerImage)
    assertEquals(Version("0.1.0"), actual.integrationLauncherConfig.protocolVersion)
    assertEquals(false, actual.integrationLauncherConfig.isCustomConnector)
    assertEquals(0L, actual.integrationLauncherConfig.attemptId)
  }

  @Test
  fun `getDiscoveryInputWithJobId for Source by actorId returns DiscoverCatalogInput`() {
    val mockActor =
      mockk<io.airbyte.data.repositories.entities.Actor> {
        every { actorType } returns ActorType.source
      }

    val mockSource = mockk<SourceConnection>()
    every { mockSource.sourceId } returns sourceId
    every { mockSource.sourceDefinitionId } returns sourceDefinitionId
    every { mockSource.workspaceId } returns workspaceId
    every { mockSource.configuration } returns configuration

    val mockSourceDefinition = mockk<StandardSourceDefinition>()
    every { mockSourceDefinition.sourceDefinitionId } returns sourceDefinitionId
    every { mockSourceDefinition.custom } returns false
    every { mockSourceDefinition.resourceRequirements } returns null

    val mockActorDefinitionVersion = mockk<ActorDefinitionVersion>()
    every { mockActorDefinitionVersion.dockerImageTag } returns dockerImageTag
    every { mockActorDefinitionVersion.dockerRepository } returns dockerRepository
    every { mockActorDefinitionVersion.protocolVersion } returns "0.1.0"
    every { mockActorDefinitionVersion.allowedHosts } returns emptyAllowedHosts
    every { mockActorDefinitionVersion.spec } returns ConnectorSpecification()

    every { actorRepository.findByActorId(sourceId) } returns mockActor
    every { sourceService.getSourceConnection(sourceId) } returns mockSource
    every { sourceService.getStandardSourceDefinition(sourceDefinitionId) } returns mockSourceDefinition
    every { actorDefinitionVersionHelper.getSourceVersion(mockSourceDefinition, workspaceId, sourceId) } returns mockActorDefinitionVersion
    every { oAuthConfigSupplier.injectSourceOAuthParameters(any(), any(), any(), any()) } returns configuration
    every { configInjector.injectConfig(any(), any()) } returns configuration
    every { secretReferenceService.getConfigWithSecretReferences(any(), any(), any()) } returns mockk { every { config } returns configuration }
    every { contextBuilder.fromSource(any()) } returns mockk()
    every { scopedConfigurationService.getScopedConfigurations(any(), any()) } returns emptyList()

    val jobId = "job-id"
    val attemptId = 1337L

    val actual = jobInputService.getDiscoveryInputWithJobId(sourceId, workspaceId, jobId, attemptId)

    assertEquals(jobId, actual.jobRunConfig.jobId)
    assertEquals(attemptId, actual.jobRunConfig.attemptId)

    assertEquals(sourceId.toString(), actual.discoverCatalogInput.sourceId)
    assertEquals(dockerImageTag, actual.discoverCatalogInput.connectorVersion)
    assertEquals(configuration, actual.discoverCatalogInput.connectionConfiguration)
    assertEquals(null, actual.discoverCatalogInput.resourceRequirements)
    assertEquals(true, actual.discoverCatalogInput.manual)

    assertEquals(workspaceId, actual.integrationLauncherConfig.workspaceId)
    assertEquals(testDockerImage, actual.integrationLauncherConfig.dockerImage)
    assertEquals(Version("0.1.0"), actual.integrationLauncherConfig.protocolVersion)
    assertEquals(false, actual.integrationLauncherConfig.isCustomConnector)
    assertEquals(attemptId, actual.integrationLauncherConfig.attemptId)
    assertEquals(jobId, actual.integrationLauncherConfig.jobId)
  }
}
