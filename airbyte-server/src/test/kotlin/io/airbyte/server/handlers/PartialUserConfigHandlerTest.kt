/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.PartialSourceUpdate
import io.airbyte.api.model.generated.SourceCreate
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.commons.constants.AirbyteSecretConstants.SECRETS_MASK
import io.airbyte.commons.json.Jsons
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
import java.util.UUID

class PartialUserConfigHandlerTest {
  private lateinit var partialUserConfigService: PartialUserConfigService
  private lateinit var configTemplateService: ConfigTemplateService
  private lateinit var sourceHandler: SourceHandler
  private lateinit var secretsProcessor: JsonSecretsProcessor
  private lateinit var handler: PartialUserConfigHandler
  private lateinit var objectMapper: ObjectMapper

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
    every { secretsProcessor.prepareSecretsForOutput(any(), any()) } returns
      configTemplate.configTemplate.partialDefaultConfig

    val sourceCreateSlot = slot<SourceCreate>()
    every { sourceHandler.createSource(capture(sourceCreateSlot)) } returns savedSource

    val result = handler.createSourceFromPartialConfig(partialUserConfigCreate, objectMapper.createObjectNode())

    Assertions.assertNotNull(result)
    Assertions.assertEquals(sourceId, result.sourceId)

    verify { partialUserConfigService.createPartialUserConfig(any()) }
    verify { sourceHandler.createSource(any()) }
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
      secretsProcessor.prepareSecretsForOutput(storedConfigWithSecrets, configTemplate.configTemplate.userConfigSpec.connectionSpecification)
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
    every { sourceHandler.partialUpdateSource(capture(sourceUpdateSlot)) } returns createMockSourceRead(sourceId)

    val result = handler.updateSourceFromPartialConfig(partialUserConfigRequest, updateConnectionConfig)

    Assertions.assertEquals(sourceId, result.sourceId)
    verify { sourceHandler.partialUpdateSource(any()) }

    val capturedConnectionConfig = sourceUpdateSlot.captured.connectionConfiguration

    Assertions.assertNotNull(capturedConnectionConfig)
    Assertions.assertTrue(capturedConnectionConfig.has("testKey"))
    Assertions.assertEquals("updatedValue", capturedConnectionConfig.get("testKey").asText())
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

  private fun createMockSourceRead(id: UUID): SourceRead =
    SourceRead().apply {
      sourceId = id
      name = "test-source"
      icon = "test-icon"
      sourceDefinitionId = UUID.randomUUID()
      connectionConfiguration = objectMapper.createObjectNode()
    }
}
