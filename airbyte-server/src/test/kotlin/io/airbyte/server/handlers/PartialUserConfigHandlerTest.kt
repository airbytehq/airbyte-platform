/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.AirbyteStream
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionScheduleData
import io.airbyte.api.model.generated.ConnectionScheduleType
import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.api.model.generated.DestinationReadList
import io.airbyte.api.model.generated.DestinationSyncMode
import io.airbyte.api.model.generated.NonBreakingChangesPreference
import io.airbyte.api.model.generated.PartialSourceUpdate
import io.airbyte.api.model.generated.SourceCreate
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.api.model.generated.SyncMode
import io.airbyte.commons.constants.AirbyteSecretConstants.SECRETS_MASK
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.ConnectionsHandler
import io.airbyte.commons.server.handlers.DestinationHandler
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConfigTemplate
import io.airbyte.config.ConfigTemplateWithActorDetails
import io.airbyte.config.PartialUserConfig
import io.airbyte.config.PartialUserConfigWithActorDetails
import io.airbyte.config.PartialUserConfigWithConfigTemplateAndActorDetails
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.secrets.JsonSecretsProcessor
import io.airbyte.data.repositories.ConnectionTemplateRepository
import io.airbyte.data.repositories.WorkspaceRepository
import io.airbyte.data.repositories.entities.ConnectionTemplate
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.data.services.PartialUserConfigService
import io.airbyte.db.instance.configs.jooq.generated.enums.NamespaceDefinitionType
import io.airbyte.db.instance.configs.jooq.generated.enums.NonBreakingChangePreferenceType
import io.airbyte.db.instance.configs.jooq.generated.enums.ScheduleType
import io.airbyte.persistence.job.WorkspaceHelper
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.publicApi.server.generated.models.JobResponse
import io.airbyte.publicApi.server.generated.models.JobStatusEnum
import io.airbyte.publicApi.server.generated.models.JobTypeEnum
import io.airbyte.server.apis.publicapi.helpers.DataResidencyHelper
import io.airbyte.server.apis.publicapi.services.JobService
import io.airbyte.server.apis.publicapi.services.SourceService
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

class PartialUserConfigHandlerTest {
  private lateinit var partialUserConfigService: PartialUserConfigService
  private lateinit var configTemplateService: ConfigTemplateService
  private lateinit var sourceHandler: SourceHandler
  private lateinit var secretsProcessor: JsonSecretsProcessor
  private lateinit var handler: PartialUserConfigHandler
  private lateinit var connectionTemplateRepository: ConnectionTemplateRepository
  private lateinit var workspaceHelper: WorkspaceHelper
  private lateinit var workspaceRepository: WorkspaceRepository
  private lateinit var connectionsHandler: ConnectionsHandler
  private lateinit var destinationHandler: DestinationHandler
  private lateinit var sourceService: SourceService
  private lateinit var dataResidencyHelper: DataResidencyHelper
  private lateinit var jobService: JobService
  private lateinit var actorDefinitionService: ActorDefinitionService

  private val objectMapper: ObjectMapper = ObjectMapper()

  private val organizationId = UUID.randomUUID()
  private val workspaceId = UUID.randomUUID()
  private val configTemplateId = UUID.randomUUID()
  private val sourceId = UUID.randomUUID()
  private val partialUserConfigId = UUID.randomUUID()
  private val actorDefinitionId = UUID.randomUUID()
  private val destinationName = "test-destination"
  private val sourceName = "test-source"
  private val connectionId = UUID.randomUUID()

  private val incrementalAirbyteStream =
    io.airbyte.api.model.generated
      .AirbyteStream()
      .name("incremental-stream")
      .jsonSchema(
        objectMapper.createObjectNode().put("type", "object"),
      ).supportedSyncModes(listOf(SyncMode.INCREMENTAL, SyncMode.INCREMENTAL))
      .sourceDefinedCursor(true)

  private val streamAndConfig = AirbyteStreamAndConfiguration()

  private val airbyteCatalog =
    AirbyteCatalog().streams(
      listOf(
        streamAndConfig,
      ),
    )
  private val schemaResponse =
    SourceDiscoverSchemaRead()
      .catalog(airbyteCatalog)

  private val incrementalAirbyteStreamWithoutSourceDefinedCursor =
    io.airbyte.api.model.generated
      .AirbyteStream()
      .name("no-source-defined-cursor-stream")
      .jsonSchema(
        objectMapper.createObjectNode().put("type", "object"),
      ).supportedSyncModes(listOf(SyncMode.INCREMENTAL, SyncMode.INCREMENTAL))
      .sourceDefinedCursor(false)

  private val fullRefreshOnlyAirbyteStream =
    io.airbyte.api.model.generated
      .AirbyteStream()
      .name("full-refresh-stream")
      .jsonSchema(
        objectMapper.createObjectNode().put("type", "object"),
      ).supportedSyncModes(listOf(SyncMode.FULL_REFRESH))
      .sourceDefinedCursor(false)

  @BeforeEach
  fun setup() {
    partialUserConfigService = mockk<PartialUserConfigService>()
    configTemplateService = mockk<ConfigTemplateService>()
    sourceHandler = mockk<SourceHandler>()
    secretsProcessor = mockk<JsonSecretsProcessor>()
    connectionTemplateRepository = mockk<ConnectionTemplateRepository>()
    workspaceHelper = mockk<WorkspaceHelper>()
    workspaceRepository = mockk<WorkspaceRepository>()
    connectionsHandler = mockk<ConnectionsHandler>()
    destinationHandler = mockk<DestinationHandler>()
    sourceService = mockk<SourceService>()
    dataResidencyHelper = mockk<DataResidencyHelper>()
    jobService = mockk<JobService>()
    actorDefinitionService = mockk<ActorDefinitionService>()

    handler =
      PartialUserConfigHandler(
        partialUserConfigService,
        configTemplateService,
        sourceHandler,
        secretsProcessor,
        connectionTemplateRepository,
        workspaceHelper,
        workspaceRepository,
        connectionsHandler,
        destinationHandler,
        sourceService,
        jobService,
        actorDefinitionService,
      )

    every { workspaceHelper.getOrganizationForWorkspace(workspaceId) } returns organizationId
    every { destinationHandler.listDestinationsForWorkspace(any()) } returns DestinationReadList()
    every { connectionTemplateRepository.findByOrganizationIdAndTombstoneFalse(organizationId) } returns emptyList()
    every { actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId) } returns
      Optional.of(ActorDefinitionVersion().withSpec(ConnectorSpecification()))

    every { sourceService.getSourceSchema(sourceId, false) } returns SourceDiscoverSchemaRead().catalog(AirbyteCatalog().streams(emptyList()))

    streamAndConfig.config(
      AirbyteStreamConfiguration().syncMode(SyncMode.INCREMENTAL).destinationSyncMode(DestinationSyncMode.APPEND_DEDUP),
    )

    every { sourceService.getSourceSchema(sourceId, false) } returns schemaResponse

    every { actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId) } returns
      Optional.of(ActorDefinitionVersion().withSpec(ConnectorSpecification()))
  }

  @Test
  fun `test createPartialUserConfig with valid inputs`() {
    val configTemplate = createMockConfigTemplate(configTemplateId, actorDefinitionId)
    val partialUserConfigCreate = createMockPartialUserConfigCreate(workspaceId, configTemplateId)
    val savedPartialUserConfig = createMockPartialUserConfig(partialUserConfigId, workspaceId, configTemplateId, sourceId)
    val savedSource = createMockSourceRead(sourceId, sourceName)

    every { configTemplateService.getConfigTemplate(configTemplateId) } returns configTemplate
    every { partialUserConfigService.createPartialUserConfig(any()) } returns savedPartialUserConfig
    every { secretsProcessor.prepareSecretsForOutput(any(), any()) } returns
      configTemplate.configTemplate.partialDefaultConfig

    val sourceCreateSlot = slot<SourceCreate>()
    every { sourceHandler.createSource(capture(sourceCreateSlot)) } returns savedSource
    every { sourceService.getSourceSchema(sourceId, false) } returns SourceDiscoverSchemaRead().catalog(AirbyteCatalog().streams(emptyList()))

    val result = handler.createSourceFromPartialConfig(partialUserConfigCreate, objectMapper.createObjectNode())

    Assertions.assertNotNull(result)
    Assertions.assertEquals(sourceId, result.sourceId)

    verify { partialUserConfigService.createPartialUserConfig(any()) }
    verify { sourceHandler.createSource(any()) }
  }

  @Test
  fun `test createPartialUserConfig with connection template`() {
    runTestCreatePartialUserConfigWithConnectionTemplate(incrementalAirbyteStream, SyncMode.INCREMENTAL)
  }

  @Test
  fun `test createPartialUserConfig with source that does not support incremental sync mode`() {
    runTestCreatePartialUserConfigWithConnectionTemplate(fullRefreshOnlyAirbyteStream, SyncMode.FULL_REFRESH)
  }

  @Test
  fun `test createPartialUserConfig with source that does not have a source defined cursor`() {
    runTestCreatePartialUserConfigWithConnectionTemplate(incrementalAirbyteStreamWithoutSourceDefinedCursor, SyncMode.FULL_REFRESH)
  }

  @Test
  fun `test combineDefaultAndUserConfig with secrets`() {
    val storedConfigJsonString =
      """
      {
        "credentials": {
          "client_id": { "_secret": "some-client-id-secret-reference" },
          "client_secret": { "_secret": "some-client-secret-secret-reference" }
        }
      }
      """.trimIndent()
    val storedConfigWithSecrets = Jsons.deserialize(storedConfigJsonString)
    val configWithObfuscatedSecretsJsonString =
      """
      {
        "credentials": {
          "client_id": "$SECRETS_MASK",
          "client_secret": "$SECRETS_MASK"
        }
      }
      """.trimIndent()
    val configWithObfuscatedSecrets = Jsons.deserialize(configWithObfuscatedSecretsJsonString)
    val configTemplate =
      createMockConfigTemplate(
        configTemplateId,
        actorDefinitionId,
        storedConfigWithSecrets,
      )
    val userConfig = Jsons.deserialize("""{"credentials":{"refresh_token":"some-refresh-token"}}""")
    every {
      secretsProcessor.prepareSecretsForOutput(
        storedConfigWithSecrets,
        configTemplate.configTemplate.userConfigSpec.connectionSpecification,
      )
    } returns configWithObfuscatedSecrets

    val result = handler.combineDefaultAndUserConfig(configTemplate.configTemplate, userConfig)

    Assertions.assertEquals(
      "some-refresh-token",
      result
        .get("credentials")
        .get("refresh_token")
        .asText(),
    )
    Assertions.assertEquals(
      SECRETS_MASK,
      result
        .get("credentials")
        .get("client_id")
        .asText(),
    )
    Assertions.assertEquals(
      SECRETS_MASK,
      result
        .get("credentials")
        .get("client_secret")
        .asText(),
    )
  }

  @Test
  fun `test updatePartialUserConfig with valid inputs`() {
    val partialUserConfig =
      createMockPartialUserConfig(
        partialUserConfigId,
        workspaceId,
        configTemplateId,
        sourceId,
      )
    val partialUserConfigWithTemplate =
      createMockPartialUserConfigWithTemplate(
        id = partialUserConfigId,
        workspaceId = workspaceId,
        configTemplateId = configTemplateId,
        sourceId = sourceId,
      )

    val updateConnectionConfig = objectMapper.createObjectNode().put("testKey", "updatedValue")

    val configTemplate = createMockConfigTemplate(configTemplateId, actorDefinitionId)

    val partialUserConfigRequest =
      PartialUserConfig(
        id = partialUserConfig.partialUserConfig.id,
        workspaceId = partialUserConfig.partialUserConfig.workspaceId,
        configTemplateId = partialUserConfig.partialUserConfig.configTemplateId,
        actorId = partialUserConfig.partialUserConfig.actorId,
      )

    every { partialUserConfigService.getPartialUserConfig(partialUserConfigId) } returns partialUserConfigWithTemplate
    every { configTemplateService.getConfigTemplate(configTemplateId) } returns configTemplate

    val sourceUpdateSlot = slot<PartialSourceUpdate>()
    every { sourceHandler.partialUpdateSource(capture(sourceUpdateSlot)) } returns createMockSourceRead(sourceId, sourceName)

    val result = handler.updateSourceFromPartialConfig(partialUserConfigRequest, updateConnectionConfig)

    Assertions.assertEquals(sourceId, result.sourceId)
    verify { sourceHandler.partialUpdateSource(any()) }

    val capturedConnectionConfig = sourceUpdateSlot.captured.connectionConfiguration

    Assertions.assertNotNull(capturedConnectionConfig)
    Assertions.assertTrue(capturedConnectionConfig.has("testKey"))
    Assertions.assertEquals("updatedValue", capturedConnectionConfig.get("testKey").asText())
  }

  @Test
  fun `test delete partial user config also deletes its source`() {
    val partialUserConfigId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()

    val partialUserConfig =
      PartialUserConfigWithConfigTemplateAndActorDetails(
        partialUserConfig =
          PartialUserConfig(
            id = partialUserConfigId,
            workspaceId = workspaceId,
            configTemplateId = configTemplateId,
            actorId = sourceId,
          ),
        actorName = "test-source",
        actorIcon = "test-icon",
        configTemplate =
          ConfigTemplate(
            id = configTemplateId,
            actorDefinitionId = actorDefinitionId,
            partialDefaultConfig = objectMapper.createObjectNode(),
            organizationId = organizationId,
            userConfigSpec = ConnectorSpecification().withConnectionSpecification(objectMapper.readTree("{}")),
          ),
      )

    every { partialUserConfigService.getPartialUserConfig(partialUserConfigId) } returns partialUserConfig
    every { partialUserConfigService.deletePartialUserConfig(partialUserConfigId) } returns Unit
    every { sourceHandler.deleteSource(SourceIdRequestBody().sourceId(sourceId)) } returns Unit

    handler.deletePartialUserConfig(partialUserConfigId)

    verify(exactly = 1) { sourceHandler.deleteSource(SourceIdRequestBody().sourceId(sourceId)) }
  }

  /**
   * helper functions for testing
   */
  private fun createMockConfigTemplate(
    id: UUID,
    actorDefinitionId: UUID = UUID.randomUUID(),
    partialDefaultConfig: JsonNode = objectMapper.createObjectNode(),
    userConfigSpec: ConnectorSpecification = ConnectorSpecification().withConnectionSpecification(objectMapper.readTree("{}")),
  ): ConfigTemplateWithActorDetails =
    ConfigTemplateWithActorDetails(
      ConfigTemplate(
        id = id,
        actorDefinitionId = actorDefinitionId,
        partialDefaultConfig = partialDefaultConfig,
        organizationId = UUID.randomUUID(),
        userConfigSpec = userConfigSpec,
        createdAt = null,
        updatedAt = null,
      ),
      actorName = "test-source",
      actorIcon = "test-icon",
    )

  private fun createMockPartialUserConfigCreate(
    workspaceId: UUID,
    configTemplateId: UUID,
  ): PartialUserConfig =
    PartialUserConfig(
      workspaceId = workspaceId,
      configTemplateId = configTemplateId,
      id = UUID.randomUUID(),
    )

  private fun createMockPartialUserConfig(
    id: UUID,
    workspaceId: UUID,
    configTemplateId: UUID,
    sourceId: UUID? = null,
  ): PartialUserConfigWithActorDetails =
    PartialUserConfigWithActorDetails(
      partialUserConfig =
        PartialUserConfig(
          id = id,
          workspaceId = workspaceId,
          configTemplateId = configTemplateId,
          actorId = sourceId,
        ),
      actorName = "test-source",
      actorIcon = "test-icon",
      configTemplateId = configTemplateId,
    )

  private fun createMockPartialUserConfigWithTemplate(
    id: UUID,
    workspaceId: UUID,
    configTemplateId: UUID,
    sourceId: UUID? = null,
    configTemplate: ConfigTemplate =
      ConfigTemplate(
        id = configTemplateId,
        actorDefinitionId = UUID.randomUUID(),
        partialDefaultConfig = objectMapper.createObjectNode(),
        organizationId = UUID.randomUUID(),
        userConfigSpec = ConnectorSpecification().withConnectionSpecification(objectMapper.readTree("{}")),
      ),
  ): PartialUserConfigWithConfigTemplateAndActorDetails =
    PartialUserConfigWithConfigTemplateAndActorDetails(
      partialUserConfig =
        PartialUserConfig(
          id = id,
          workspaceId = workspaceId,
          configTemplateId = configTemplateId,
          actorId = sourceId,
        ),
      configTemplate = configTemplate,
      actorName = "test-source",
      actorIcon = "test-icon",
    )

  private fun createMockSourceRead(
    id: UUID,
    sourceName: String,
  ): SourceRead =
    SourceRead().apply {
      sourceId = id
      name = sourceName
      icon = "test-icon"
      sourceDefinitionId = UUID.randomUUID()
      connectionConfiguration = objectMapper.createObjectNode()
    }

  private fun runTestCreatePartialUserConfigWithConnectionTemplate(
    airbyteStream: AirbyteStream,
    expectedSyncMode: SyncMode,
  ) {
    val configTemplate = createMockConfigTemplate(configTemplateId, actorDefinitionId)
    val partialUserConfigCreate = createMockPartialUserConfigCreate(workspaceId, configTemplateId)
    val savedPartialUserConfig = createMockPartialUserConfig(partialUserConfigId, workspaceId, configTemplateId, sourceId)
    val savedSource = createMockSourceRead(sourceId, sourceName)

    val destinationId = UUID.randomUUID()
    val streamAndConfig = AirbyteStreamAndConfiguration()
    streamAndConfig
      .config(
        AirbyteStreamConfiguration().syncMode(expectedSyncMode).destinationSyncMode(DestinationSyncMode.APPEND_DEDUP),
      ).stream(
        airbyteStream,
      )
    val airbyteCatalog =
      AirbyteCatalog().streams(
        listOf(
          streamAndConfig,
        ),
      )

    every { destinationHandler.listDestinationsForWorkspace(any()) } returns
      DestinationReadList()
        .destinations(
          listOf(
            DestinationRead()
              .destinationId(destinationId)
              .name(destinationName)
              .workspaceId(workspaceId),
          ),
        )

    val schemaResponse =
      SourceDiscoverSchemaRead()
        .catalog(airbyteCatalog)

    val expectedAirbyteStreamAndConfig = AirbyteStreamAndConfiguration()
    expectedAirbyteStreamAndConfig
      .config(AirbyteStreamConfiguration().syncMode(expectedSyncMode).destinationSyncMode(DestinationSyncMode.APPEND))
      .stream(
        airbyteStream,
      )

    val connectionRead = ConnectionRead().connectionId(connectionId)
    every {
      connectionsHandler.createConnection(
        match {
          it.name == "$sourceName -> $destinationName" &&
            it.namespaceDefinition == io.airbyte.api.model.generated.NamespaceDefinitionType.DESTINATION &&
            it.sourceId == sourceId &&
            it.destinationId == destinationId &&
            it.syncCatalog == AirbyteCatalog().streams(listOf(expectedAirbyteStreamAndConfig)) &&
            it.scheduleType == ConnectionScheduleType.MANUAL &&
            it.scheduleData == ConnectionScheduleData() &&
            it.status == ConnectionStatus.ACTIVE &&
            it.nonBreakingChangesPreference == NonBreakingChangesPreference.IGNORE
        },
      )
    } returns connectionRead

    every { sourceService.getSourceSchema(sourceId, false) } returns schemaResponse

    val connectionTemplate =
      ConnectionTemplate(
        id = UUID.randomUUID(),
        organizationId = organizationId,
        destinationName = destinationName,
        destinationDefinitionId = UUID.randomUUID(),
        destinationConfig = objectMapper.createObjectNode(),
        namespaceDefinition = NamespaceDefinitionType.destination,
        namespaceFormat = null,
        prefix = null,
        scheduleType = ScheduleType.manual,
        scheduleData = null,
        nonBreakingChangesPreference = NonBreakingChangePreferenceType.ignore,
        resourceRequirements =
          Jsons.jsonNode(
            ResourceRequirements()
              .withCpuLimit(
                "1",
              ).withMemoryLimit("1g")
              .withCpuRequest("0.5")
              .withMemoryRequest("0.5g")
              .withEphemeralStorageLimit("1g")
              .withEphemeralStorageRequest("0.5g"),
          ),
      )
    every { connectionTemplateRepository.findByOrganizationIdAndTombstoneFalse(organizationId) } returns listOf(connectionTemplate)

    every { configTemplateService.getConfigTemplate(configTemplateId) } returns configTemplate
    every { partialUserConfigService.createPartialUserConfig(any()) } returns savedPartialUserConfig
    every { secretsProcessor.prepareSecretsForOutput(any(), any()) } returns
      configTemplate.configTemplate.partialDefaultConfig

    val sourceCreateSlot = slot<SourceCreate>()
    every { sourceHandler.createSource(capture(sourceCreateSlot)) } returns savedSource

    every { jobService.sync(connectionId) } returns
      JobResponse(
        jobId = 2L,
        status = JobStatusEnum.PENDING,
        jobType = JobTypeEnum.SYNC,
        startTime = "",
        connectionId = connectionId.toString(),
      )

    val result = handler.createSourceFromPartialConfig(partialUserConfigCreate, objectMapper.createObjectNode())

    Assertions.assertNotNull(result)
    Assertions.assertEquals(sourceId, result.sourceId)

    verify { partialUserConfigService.createPartialUserConfig(any()) }
    verify { sourceHandler.createSource(any()) }
  }
}
