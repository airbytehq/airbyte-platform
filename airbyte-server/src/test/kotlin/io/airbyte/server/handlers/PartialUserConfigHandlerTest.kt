/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.model.generated.PartialSourceUpdate
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.config.ConfigTemplate
import io.airbyte.config.ConfigTemplateWithActorDetails
import io.airbyte.config.PartialUserConfig
import io.airbyte.config.PartialUserConfigWithActorDetails
import io.airbyte.config.PartialUserConfigWithConfigTemplateAndActorDetails
import io.airbyte.config.secrets.JsonSecretsProcessor
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.data.services.PartialUserConfigService
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class PartialUserConfigHandlerTest {
  private lateinit var partialUserConfigService: PartialUserConfigService
  private lateinit var configTemplateService: ConfigTemplateService
  private lateinit var sourceHandler: SourceHandler
  private lateinit var handler: PartialUserConfigHandler
  private lateinit var objectMapper: ObjectMapper
  private lateinit var secretsProcessor: JsonSecretsProcessor

  private val workspaceId = UUID.randomUUID()
  private val configTemplateId = UUID.randomUUID()
  private val sourceId = UUID.randomUUID()
  private val partialUserConfigId = UUID.randomUUID()
  private val actorDefinitionId = UUID.randomUUID()

  @BeforeEach
  fun setup() {
    partialUserConfigService = mockk<PartialUserConfigService>()
    configTemplateService = mockk<ConfigTemplateService>()
    sourceHandler = mockk<SourceHandler>()
    secretsProcessor = mockk<JsonSecretsProcessor>()
    handler = PartialUserConfigHandler(partialUserConfigService, configTemplateService, sourceHandler, secretsProcessor)
    objectMapper = ObjectMapper()
  }

  @Test
  fun `test createPartialUserConfig with valid inputs`() {
    val configTemplate = createMockConfigTemplate(configTemplateId, actorDefinitionId)
    val partialUserConfigCreate = createMockPartialUserConfigCreate(workspaceId, configTemplateId)
    val savedPartialUserConfig = createMockPartialUserConfig(partialUserConfigId, workspaceId, configTemplateId, sourceId)
    val savedSource = createMockSourceRead(sourceId)

    every { configTemplateService.getConfigTemplate(configTemplateId) } returns configTemplate
    every { partialUserConfigService.createPartialUserConfig(any()) } returns savedPartialUserConfig
    every { sourceHandler.createSource(any()) } returns savedSource
    every { sourceHandler.persistConfigRawSecretValues(any(), any(), any(), any(), any()) } returns
      partialUserConfigCreate.connectionConfiguration

    val result = handler.createPartialUserConfig(partialUserConfigCreate)

    Assertions.assertNotNull(result)
    Assertions.assertEquals(partialUserConfigId, result.partialUserConfig.id)
    Assertions.assertEquals(sourceId, result.partialUserConfig.actorId)

    verify { partialUserConfigService.createPartialUserConfig(any()) }
    verify { sourceHandler.createSource(any()) }
  }

  @Test
  fun `test createPartialUserConfig does not create config if creating source fails`() {
    // Arrange
    val configTemplate = createMockConfigTemplate(configTemplateId, actorDefinitionId)
    val partialUserConfigCreate = createMockPartialUserConfigCreate(workspaceId, configTemplateId)

    every { configTemplateService.getConfigTemplate(configTemplateId) } returns configTemplate

    every { sourceHandler.createSource(any()) } throws RuntimeException("Source creation failed")

    assertThrows<RuntimeException> {
      handler.createPartialUserConfig(partialUserConfigCreate)
    }

    // Verify that the partial user config service was never called to write the config
    verify(exactly = 0) { partialUserConfigService.createPartialUserConfig(any()) }
  }

  @Test
  fun `test updatePartialUserConfig with valid inputs`() {
    val initialConnectionConfig = objectMapper.createObjectNode().put("testKey", "initialValue")

    val initialProperties = initialConnectionConfig

    val partialUserConfig =
      createMockPartialUserConfig(
        partialUserConfigId,
        workspaceId,
        configTemplateId,
        sourceId,
        initialProperties,
      )
    val partialUserConfigWithTemplate =
      createMockPartialUserConfigWithTemplate(
        id = partialUserConfigId,
        workspaceId = workspaceId,
        configTemplateId = configTemplateId,
        sourceId = sourceId,
        properties = initialProperties,
      )

    val updateConnectionConfig = objectMapper.createObjectNode().put("testKey", "updatedValue")

    val configTemplate = createMockConfigTemplate(configTemplateId, actorDefinitionId)

    val partialUserConfigRequest =
      PartialUserConfig(
        id = partialUserConfig.partialUserConfig.id,
        workspaceId = partialUserConfig.partialUserConfig.workspaceId,
        configTemplateId = partialUserConfig.partialUserConfig.configTemplateId,
        actorId = partialUserConfig.partialUserConfig.actorId,
        connectionConfiguration = updateConnectionConfig,
      )

    every {
      sourceHandler.persistConfigRawSecretValues(
        any(),
        any(),
        any(),
        any<ConnectorSpecification>(),
        any(),
      )
    } returns updateConnectionConfig

    val configSlot = slot<PartialUserConfig>()
    every { partialUserConfigService.updatePartialUserConfig(capture(configSlot)) } returns partialUserConfig

    every { partialUserConfigService.getPartialUserConfig(partialUserConfigId) } returns partialUserConfigWithTemplate
    every { configTemplateService.getConfigTemplate(configTemplateId) } returns configTemplate

    val sourceUpdateSlot = slot<PartialSourceUpdate>()
    every { sourceHandler.partialUpdateSource(capture(sourceUpdateSlot)) } returns createMockSourceRead(sourceId)

    val result = handler.updatePartialUserConfig(partialUserConfigRequest)

    Assertions.assertEquals(partialUserConfigId, result.partialUserConfig.id)
    Assertions.assertEquals(sourceId, result.partialUserConfig.actorId)
    verify { partialUserConfigService.updatePartialUserConfig(any()) }
    verify { sourceHandler.partialUpdateSource(any()) }

    val capturedConnectionConfig = sourceUpdateSlot.captured.connectionConfiguration
    val capturedUserConfig = configSlot.captured.connectionConfiguration

    Assertions.assertNotNull(capturedConnectionConfig)
    Assertions.assertTrue(capturedConnectionConfig.has("testKey"))
    Assertions.assertEquals("updatedValue", capturedConnectionConfig.get("testKey").asText())
    Assertions.assertEquals("updatedValue", capturedUserConfig.get("testKey").asText())
  }

  /**
   * helper functions for testing
   */
  private fun createMockConfigTemplate(
    id: UUID,
    actorDefinitionId: UUID = UUID.randomUUID(),
    partialDefaultConfig: JsonNode = objectMapper.createObjectNode(),
  ): ConfigTemplateWithActorDetails =
    ConfigTemplateWithActorDetails(
      ConfigTemplate(
        id = id,
        actorDefinitionId = actorDefinitionId,
        partialDefaultConfig = partialDefaultConfig,
        organizationId = UUID.randomUUID(),
        userConfigSpec = ConnectorSpecification().withConnectionSpecification(objectMapper.readTree("{}")),
        createdAt = null,
        updatedAt = null,
      ),
      actorName = "test-source",
      actorIcon = "test-icon",
    )

  private fun createMockPartialUserConfigCreate(
    workspaceId: UUID,
    configTemplateId: UUID,
    userConfig: JsonNode = objectMapper.createObjectNode().set<ObjectNode>("connectionConfiguration", objectMapper.createObjectNode()),
  ): PartialUserConfig =
    PartialUserConfig(
      workspaceId = workspaceId,
      configTemplateId = configTemplateId,
      connectionConfiguration = userConfig,
      id = UUID.randomUUID(),
    )

  private fun createMockPartialUserConfig(
    id: UUID,
    workspaceId: UUID,
    configTemplateId: UUID,
    sourceId: UUID? = null,
    properties: JsonNode = objectMapper.createObjectNode(),
  ): PartialUserConfigWithActorDetails =
    PartialUserConfigWithActorDetails(
      partialUserConfig =
        PartialUserConfig(
          id = id,
          workspaceId = workspaceId,
          configTemplateId = configTemplateId,
          connectionConfiguration = properties,
          actorId = sourceId,
        ),
      actorName = "test-source",
      actorIcon = "test-icon",
    )

  private fun createMockPartialUserConfigWithTemplate(
    id: UUID,
    workspaceId: UUID,
    configTemplateId: UUID,
    sourceId: UUID? = null,
    properties: JsonNode = objectMapper.createObjectNode(),
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
          connectionConfiguration = properties,
          actorId = sourceId,
        ),
      configTemplate = configTemplate,
      actorName = "test-source",
      actorIcon = "test-icon",
    )

  private fun createMockSourceRead(id: UUID): SourceRead =
    SourceRead().apply {
      sourceId = id
      name = "test-source"
      sourceDefinitionId = UUID.randomUUID()
      connectionConfiguration = objectMapper.createObjectNode()
    }
}
