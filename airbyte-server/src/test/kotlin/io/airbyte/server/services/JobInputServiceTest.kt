/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.hash.Hashing
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.helpers.ContextBuilder
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorContext
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.AllowedHosts
import io.airbyte.config.Attempt
import io.airbyte.config.AttemptSyncConfig
import io.airbyte.config.CatalogDiff
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConnectionContext
import io.airbyte.config.DestinationConnection
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.ScopedResourceRequirements
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.SyncResourceRequirements
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.ConfigInjector
import io.airbyte.config.secrets.ConfigWithSecretReferences
import io.airbyte.data.repositories.ActorDefinitionRepository
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.repositories.entities.ActorDefinition
import io.airbyte.data.services.AttemptService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.JobService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SourceService
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.airbyte.domain.services.secrets.SecretReferenceService
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.persistence.job.factory.OAuthConfigSupplier
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.RefreshSchemaActivityOutput
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.models.ReplicationFeatureFlags
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
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
  private lateinit var connectionService: ConnectionService
  private lateinit var jobService: JobService
  private lateinit var replicationFeatureFlags: ReplicationFeatureFlags
  private lateinit var featureFlagClient: FeatureFlagClient
  private lateinit var attemptService: AttemptService
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
  private val connectionId = UUID.randomUUID()
  private val sourceConfiguration: JsonNode = Jsons.jsonNode(mapOf("source" to "configuration"))
  private val destinationConfiguration: JsonNode = Jsons.jsonNode(mapOf("destination" to "configuration"))
  private val protocolVersion = "1.2.3"
  private val jobId = 321L
  private val attemptNumber = 3
  private val signalInput = "signalInput"
  private val destinationImage = "destination/image"
  private val destinationImageTag = "2.0"

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
    connectionService = mockk()
    jobService = mockk()
    replicationFeatureFlags = mockk()
    featureFlagClient = mockk()
    attemptService = mockk()

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
        connectionService,
        jobService,
        replicationFeatureFlags,
        featureFlagClient,
        attemptService,
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
    every { secretReferenceService.getConfigWithSecretReferences(any(), any(), any()) } returns
      ConfigWithSecretReferences(configuration, mapOf())
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
    every { secretReferenceService.getConfigWithSecretReferences(any(), any(), any()) } returns
      ConfigWithSecretReferences(configuration, mapOf())
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
    every { secretReferenceService.getConfigWithSecretReferences(any(), any(), any()) } returns
      mockk { every { originalConfig } returns configuration }
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
    every { secretReferenceService.getConfigWithSecretReferences(any(), any(), any()) } returns
      mockk { every { originalConfig } returns configuration }
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
  fun `getDiscoverInput for Source by actorId returns DiscoverCatalogInput`() {
    val configurationWithSecretRef = Jsons.jsonNode(mapOf("test" to "secret-reference"))
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
    every { secretReferenceService.getConfigWithSecretReferences(any(), any(), any()) } returns
      ConfigWithSecretReferences(configurationWithSecretRef, mapOf())
    every { contextBuilder.fromSource(any()) } returns mockk()
    every { scopedConfigurationService.getScopedConfigurations(any(), any()) } returns emptyList()

    val actual = jobInputService.getDiscoverInput(sourceId)

    assertEquals(0L, actual.jobRunConfig.attemptId)

    assertEquals(sourceId.toString(), actual.discoverCatalogInput.sourceId)
    assertEquals(dockerImageTag, actual.discoverCatalogInput.connectorVersion)
    assertEquals(configurationWithSecretRef, actual.discoverCatalogInput.connectionConfiguration)
    assertEquals(null, actual.discoverCatalogInput.resourceRequirements)
    assertEquals(true, actual.discoverCatalogInput.manual)

    assertEquals(workspaceId, actual.integrationLauncherConfig.workspaceId)
    assertEquals(testDockerImage, actual.integrationLauncherConfig.dockerImage)
    assertEquals(Version("0.1.0"), actual.integrationLauncherConfig.protocolVersion)
    assertEquals(false, actual.integrationLauncherConfig.isCustomConnector)
    assertEquals(0L, actual.integrationLauncherConfig.attemptId)

    val expectedConfigHash = Hashing.md5().hashBytes(Jsons.serialize(configuration).toByteArray(Charsets.UTF_8)).toString()
    assertEquals(expectedConfigHash, actual.discoverCatalogInput.configHash)
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
    every { secretReferenceService.getConfigWithSecretReferences(any(), any(), any()) } returns
      ConfigWithSecretReferences(configuration, mapOf())
    every { contextBuilder.fromSource(any()) } returns mockk()
    every { scopedConfigurationService.getScopedConfigurations(any(), any()) } returns emptyList()

    val jobId = "job-id"
    val attemptId = 1337L

    val actual = jobInputService.getDiscoverInput(sourceId, jobId, attemptId)

    assertEquals(jobId, actual.jobRunConfig.jobId)
    assertEquals(attemptId, actual.jobRunConfig.attemptId)

    assertEquals(sourceId.toString(), actual.discoverCatalogInput.sourceId)
    assertEquals(dockerImageTag, actual.discoverCatalogInput.connectorVersion)
    assertEquals(configuration, actual.discoverCatalogInput.connectionConfiguration)
    assertEquals(null, actual.discoverCatalogInput.resourceRequirements)
    assertEquals(false, actual.discoverCatalogInput.manual)

    assertEquals(workspaceId, actual.integrationLauncherConfig.workspaceId)
    assertEquals(testDockerImage, actual.integrationLauncherConfig.dockerImage)
    assertEquals(Version("0.1.0"), actual.integrationLauncherConfig.protocolVersion)
    assertEquals(false, actual.integrationLauncherConfig.isCustomConnector)
    assertEquals(attemptId, actual.integrationLauncherConfig.attemptId)
    assertEquals(jobId, actual.integrationLauncherConfig.jobId)
  }

  @EnumSource(JobConfig.ConfigType::class, names = ["SYNC", "RESET_CONNECTION", "REFRESH", "CLEAR"])
  @ParameterizedTest
  fun `getReplicationInput returns correct ReplicationActivityInput`(configType: JobConfig.ConfigType) {
    val mockConnection = mockk<StandardSync>()
    every { mockConnection.connectionId } returns connectionId
    every { mockConnection.sourceId } returns sourceId
    every { mockConnection.destinationId } returns destinationId

    val mockSource = mockk<SourceConnection>()
    every { mockSource.sourceId } returns sourceId
    every { mockSource.sourceDefinitionId } returns sourceDefinitionId
    every { mockSource.workspaceId } returns workspaceId
    every { mockSource.configuration } returns sourceConfiguration

    val mockSourceDefinition = mockk<StandardSourceDefinition>()
    every { mockSourceDefinition.sourceDefinitionId } returns sourceDefinitionId
    every { mockSourceDefinition.custom } returns false
    every { mockSourceDefinition.sourceType } returns StandardSourceDefinition.SourceType.DATABASE

    val sourceICPOption =
      Jsons.jsonNode(
        mapOf(
          "source" to "ICPOption",
        ),
      )
    val mockSourceDefinitionVersion = mockk<ActorDefinitionVersion>()
    every { mockSourceDefinitionVersion.dockerRepository } returns dockerRepository
    every { mockSourceDefinitionVersion.dockerImageTag } returns dockerImageTag
    every { mockSourceDefinitionVersion.protocolVersion } returns protocolVersion
    every { mockSourceDefinitionVersion.allowedHosts } returns AllowedHosts()
    every { mockSourceDefinitionVersion.connectorIPCOptions } returns sourceICPOption

    val mockDestination = mockk<DestinationConnection>()
    every { mockDestination.destinationId } returns destinationId
    every { mockDestination.destinationDefinitionId } returns destinationDefinitionId
    every { mockDestination.workspaceId } returns workspaceId
    every { mockDestination.configuration } returns destinationConfiguration

    val destinationICPOption =
      Jsons.jsonNode(
        mapOf(
          "destination" to "ICPOption",
        ),
      )
    val mockDestinationDefinition = mockk<StandardDestinationDefinition>()
    every { mockDestinationDefinition.destinationDefinitionId } returns destinationDefinitionId
    every { mockDestinationDefinition.custom } returns true

    val mockDestinationDefinitionVersion = mockk<ActorDefinitionVersion>()
    every { mockDestinationDefinitionVersion.dockerImageTag } returns destinationImageTag
    every { mockDestinationDefinitionVersion.dockerRepository } returns destinationImage
    every { mockDestinationDefinitionVersion.protocolVersion } returns protocolVersion
    every { mockDestinationDefinitionVersion.allowedHosts } returns AllowedHosts()
    every { mockDestinationDefinitionVersion.connectorIPCOptions } returns destinationICPOption
    every { mockDestinationDefinitionVersion.supportsRefreshes } returns false

    val attemptSyncConfig = mockk<AttemptSyncConfig>()
    every { attemptSyncConfig.sourceConfiguration } returns null

    val mockAttempt = mockk<Attempt>()
    every { mockAttempt.attemptNumber } returns attemptNumber
    every { mockAttempt.processingTaskQueue } returns "test_queue"
    every { mockAttempt.syncConfig } returns attemptSyncConfig

    val jobConfig = mockk<JobConfig>()
    every { jobConfig.configType } returns configType

    val expectedNamespaceDefinition = NamespaceDefinitionType.CUSTOMFORMAT
    val expectedNamespaceFormat = "$configType-format"
    val expectedPrefix = "$configType-prefix"
    when (configType) {
      JobConfig.ConfigType.SYNC ->
        every { jobConfig.sync } returns
          mockk {
            every { namespaceDefinition } returns expectedNamespaceDefinition
            every { namespaceFormat } returns expectedNamespaceFormat
            every { prefix } returns expectedPrefix
            every { syncResourceRequirements } returns SyncResourceRequirements()
            every { configuredAirbyteCatalog } returns ConfiguredAirbyteCatalog()
          }

      JobConfig.ConfigType.REFRESH ->
        every { jobConfig.refresh } returns
          mockk {
            every { namespaceDefinition } returns expectedNamespaceDefinition
            every { namespaceFormat } returns expectedNamespaceFormat
            every { prefix } returns expectedPrefix
            every { syncResourceRequirements } returns SyncResourceRequirements()
            every { configuredAirbyteCatalog } returns ConfiguredAirbyteCatalog()
          }

      JobConfig.ConfigType.CLEAR, JobConfig.ConfigType.RESET_CONNECTION ->
        every { jobConfig.resetConnection } returns
          mockk {
            every { namespaceDefinition } returns expectedNamespaceDefinition
            every { namespaceFormat } returns expectedNamespaceFormat
            every { prefix } returns expectedPrefix
            every { syncResourceRequirements } returns SyncResourceRequirements()
            every { configuredAirbyteCatalog } returns ConfiguredAirbyteCatalog()
          }

      else -> throw IllegalStateException("$configType not handled by the test setup")
    }

    val mockJob =
      Job(
        jobId,
        configType,
        connectionId.toString(),
        jobConfig,
        emptyList(),
        io.airbyte.config.JobStatus.PENDING,
        0L,
        0L,
        0L,
        true,
      )

    every { connectionService.getStandardSync(connectionId) } returns mockConnection
    every { sourceService.getSourceConnection(sourceId) } returns mockSource
    every { sourceService.getStandardSourceDefinition(sourceDefinitionId) } returns mockSourceDefinition
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        mockSourceDefinition,
        workspaceId,
        sourceId,
      )
    } returns mockSourceDefinitionVersion
    every { destinationService.getDestinationConnection(destinationId) } returns mockDestination
    every {
      destinationService.getStandardDestinationDefinition(
        destinationDefinitionId,
      )
    } returns mockDestinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        mockDestinationDefinition,
        workspaceId,
        destinationId,
      )
    } returns mockDestinationDefinitionVersion
    every { jobService.findById(jobId) } returns mockJob
    every { attemptService.getAttempt(jobId, attemptNumber.toLong()) } returns mockAttempt
    val connectionContext = mockk<ConnectionContext>()
    every { contextBuilder.fromConnectionId(connectionId) } returns connectionContext
    every { scopedConfigurationService.getScopedConfigurations(any(), any()) } returns emptyList()
    every { replicationFeatureFlags.featureFlags } returns emptyList() // or mock with relevant flags
    every { mockSourceDefinition.maxSecondsBetweenMessages } returns 3600L

    val appliedCatalogDiff = CatalogDiff()

    val isResetExpected = configType == JobConfig.ConfigType.CLEAR || configType == JobConfig.ConfigType.RESET_CONNECTION
    val expected =
      ReplicationActivityInput(
        sourceId = sourceId,
        destinationId = destinationId,
        sourceConfiguration = sourceConfiguration,
        destinationConfiguration = destinationConfiguration,
        jobRunConfig = JobRunConfig().withJobId(jobId.toString()).withAttemptId(attemptNumber.toLong()),
        sourceLauncherConfig =
          IntegrationLauncherConfig()
            .withJobId(jobId.toString())
            .withWorkspaceId(workspaceId)
            .withDockerImage("$dockerRepository:$dockerImageTag")
            .withProtocolVersion(
              Version(protocolVersion),
            ).withIsCustomConnector(false)
            .withAttemptId(attemptNumber.toLong())
            .withAllowedHosts(AllowedHosts())
            .withConnectionId(connectionId),
        destinationLauncherConfig =
          IntegrationLauncherConfig()
            .withJobId(jobId.toString())
            .withWorkspaceId(workspaceId)
            .withDockerImage("$destinationImage:$destinationImageTag")
            .withProtocolVersion(
              Version(protocolVersion),
            ).withIsCustomConnector(true)
            .withAttemptId(attemptNumber.toLong())
            .withAllowedHosts(AllowedHosts())
            .withConnectionId(connectionId),
        namespaceDefinition = expectedNamespaceDefinition,
        namespaceFormat = expectedNamespaceFormat,
        prefix = expectedPrefix,
        syncResourceRequirements = SyncResourceRequirements(),
        workspaceId = workspaceId,
        connectionId = connectionId,
        taskQueue = "test_queue",
        isReset = isResetExpected,
        connectionContext = connectionContext,
        signalInput = signalInput,
        networkSecurityTokens = emptyList(),
        includesFiles = false,
        omitFileTransferEnvVar = false,
        featureFlags = emptyMap(),
        heartbeatMaxSecondsBetweenMessages = 3600L,
        supportsRefreshes = false,
        schemaRefreshOutput = RefreshSchemaActivityOutput(appliedCatalogDiff),
        sourceIPCOptions = sourceICPOption,
        destinationIPCOptions = destinationICPOption,
      )

    val actual = jobInputService.getReplicationInput(connectionId, appliedCatalogDiff, signalInput, jobId, attemptNumber.toLong())
    assertEquals(expected, actual)
  }
}
