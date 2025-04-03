/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.PartialSourceUpdate
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.config.ConfigTemplate
import io.airbyte.config.ConfigTemplateWithActorDetails
import io.airbyte.config.PartialUserConfig
import io.airbyte.config.PartialUserConfigWithActorDetails
import io.airbyte.config.PartialUserConfigWithConfigTemplateAndActorDetails
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
    handler = PartialUserConfigHandler(partialUserConfigService, configTemplateService, sourceHandler)
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
    every { sourceHandler.persistSecretsAndUpdateSourceConnection(null, any(), any(), any()) } returns
      partialUserConfigCreate.partialUserConfigProperties

    val result = handler.createPartialUserConfig(partialUserConfigCreate)

    Assertions.assertNotNull(result)
    Assertions.assertEquals(partialUserConfigId, result.partialUserConfig.id)
    Assertions.assertEquals(sourceId, result.partialUserConfig.sourceId)

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

    val initialProperties =
      objectMapper
        .createObjectNode()
        .set<JsonNode>("connectionConfiguration", initialConnectionConfig)

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
    val updateUserConfig =
      objectMapper
        .createObjectNode()
        .set<JsonNode>("connectionConfiguration", updateConnectionConfig)

    val configTemplate = createMockConfigTemplate(configTemplateId, actorDefinitionId)

    val partialUserConfigRequest =
      PartialUserConfig(
        id = partialUserConfig.partialUserConfig.id,
        workspaceId = partialUserConfig.partialUserConfig.workspaceId,
        configTemplateId = partialUserConfig.partialUserConfig.configTemplateId,
        sourceId = partialUserConfig.partialUserConfig.sourceId,
        partialUserConfigProperties = updateUserConfig,
      )

    every {
      sourceHandler.persistSecretsAndUpdateSourceConnection(
        any(),
        any(),
        any(),
        any<ConnectorSpecification>(),
      )
    } returns
      updateUserConfig

    val configSlot = slot<PartialUserConfig>()
    every { partialUserConfigService.updatePartialUserConfig(capture(configSlot)) } returns partialUserConfig

    every { partialUserConfigService.getPartialUserConfig(partialUserConfigId) } returns partialUserConfigWithTemplate
    every { configTemplateService.getConfigTemplate(configTemplateId) } returns configTemplate

    val sourceUpdateSlot = slot<PartialSourceUpdate>()
    every { sourceHandler.partialUpdateSource(capture(sourceUpdateSlot)) } returns createMockSourceRead(sourceId)

    val result = handler.updatePartialUserConfig(partialUserConfigRequest)

    Assertions.assertEquals(partialUserConfigId, result.partialUserConfig.id)
    Assertions.assertEquals(sourceId, result.partialUserConfig.sourceId)
    verify { partialUserConfigService.updatePartialUserConfig(any()) }
    verify { sourceHandler.partialUpdateSource(any()) }

    val capturedConnectionConfig = sourceUpdateSlot.captured.connectionConfiguration
    val capturedUserConfig = configSlot.captured.partialUserConfigProperties

    Assertions.assertNotNull(capturedConnectionConfig)
    Assertions.assertTrue(capturedConnectionConfig.has("testKey"))
    Assertions.assertEquals("updatedValue", capturedConnectionConfig.get("testKey").asText())
    Assertions.assertEquals("updatedValue", capturedUserConfig.get("connectionConfiguration").get("testKey").asText())
  }

  @Test
  fun `test combineProperties merges flat configurations correctly`() {
    // Create the template config with connectionConfiguration
    val templateConfig = objectMapper.createObjectNode()
    val templateConnectionConfig = templateConfig.putObject("connectionConfiguration")
    templateConnectionConfig.put("host", "template-host")
    templateConnectionConfig.put("port", 5432)
    templateConnectionConfig.put("database", "template-db")

    // Create the user config with connectionConfiguration
    val userConfig = objectMapper.createObjectNode()
    val userConnectionConfig = userConfig.putObject("connectionConfiguration")
    userConnectionConfig.put("host", "user-host")
    userConnectionConfig.put("username", "user")
    userConnectionConfig.put("password", "secret")

    // Combine the configs
    val result = handler.combineProperties(templateConfig, userConfig)

    // Assert the results - note that result is the merged connectionConfiguration content
    Assertions.assertNotNull(result, "result should not be null")
    Assertions.assertEquals("user-host", result.get("host").asText())
    Assertions.assertEquals(5432, result.get("port").asInt())
    Assertions.assertEquals("template-db", result.get("database").asText())
    Assertions.assertEquals("user", result.get("username").asText())
    Assertions.assertEquals("secret", result.get("password").asText())
  }

  @Test
  fun `test combineProperties merges nested configurations correctly`() {
    // Create the template config with connectionConfiguration
    val templateConfig = objectMapper.createObjectNode()
    val templateConnectionConfig = templateConfig.putObject("connectionConfiguration")
    templateConnectionConfig.put("host", "template-host")
    val templateCredentials = templateConnectionConfig.putObject("credentials")
    templateCredentials.put("type", "password")
    templateCredentials.put("username", "template-user")

    // Create the user config with connectionConfiguration
    val userConfig = objectMapper.createObjectNode()
    val userConnectionConfig = userConfig.putObject("connectionConfiguration")
    userConnectionConfig.put("host", "user-host")
    val userCredentials = userConnectionConfig.putObject("credentials")
    userCredentials.put("username", "user")
    userCredentials.put("password", "secret")

    // Combine the configs
    val result = handler.combineProperties(templateConfig, userConfig)

    // Assert the results - note that result is the merged connectionConfiguration content
    Assertions.assertNotNull(result, "result should not be null")
    Assertions.assertEquals("user-host", result.get("host").asText())
    Assertions.assertEquals("user", result.get("credentials").get("username").asText())
    Assertions.assertEquals("secret", result.get("credentials").get("password").asText())
    Assertions.assertEquals("password", result.get("credentials").get("type").asText())
  }

  @Test
  fun `test combineProperties handles empty configurations`() {
    val templateConfig = objectMapper.createObjectNode()
    val userConfig = objectMapper.createObjectNode()

    val result = handler.combineProperties(templateConfig, userConfig)

    Assertions.assertTrue(result.isEmpty)
  }

  @Test
  fun `test combineProperties handles null configurations`() {
    val templateConfig = objectMapper.createObjectNode()
    val userConfig = objectMapper.nullNode()

    val result = handler.combineProperties(templateConfig, userConfig)

    Assertions.assertTrue(result.isEmpty)
  }

  @Test
  fun `test combineProperties preserves non-overlapping nested objects`() {
    // Create the template config with connectionConfiguration
    val templateConfig = objectMapper.createObjectNode()
    val templateConnectionConfig = templateConfig.putObject("connectionConfiguration")
    val templateSource = templateConnectionConfig.putObject("source")
    templateSource.put("type", "postgres")
    templateSource.put("host", "template-host")

    // Create the user config with connectionConfiguration
    val userConfig = objectMapper.createObjectNode()
    val userConnectionConfig = userConfig.putObject("connectionConfiguration")
    val userDestination = userConnectionConfig.putObject("destination")
    userDestination.put("type", "snowflake")
    userDestination.put("account", "user-account")

    // Combine the configs
    val result = handler.combineProperties(templateConfig, userConfig)

    // Assert the results - note that result is the merged connectionConfiguration content
    Assertions.assertNotNull(result, "result should not be null")
    Assertions.assertEquals("postgres", result.get("source").get("type").asText())
    Assertions.assertEquals("template-host", result.get("source").get("host").asText())
    Assertions.assertEquals("snowflake", result.get("destination").get("type").asText())
    Assertions.assertEquals("user-account", result.get("destination").get("account").asText())
  }

  @Test
  fun `test combineProperties gives precedence to user config for conflicting keys`() {
    // Create the template config with connectionConfiguration
    val templateConfig = objectMapper.createObjectNode()
    val templateConnectionConfig = templateConfig.putObject("connectionConfiguration")
    templateConnectionConfig.put("host", "template-host")
    templateConnectionConfig.put("port", 5432)
    templateConnectionConfig.put("username", "template-user")
    templateConnectionConfig.put("password", "template-password")
    val templateAdvanced = templateConnectionConfig.putObject("advanced")
    templateAdvanced.put("ssl", true)
    templateAdvanced.put("timeout", 30)

    // Create the user config with connectionConfiguration
    val userConfig = objectMapper.createObjectNode()
    val userConnectionConfig = userConfig.putObject("connectionConfiguration")
    userConnectionConfig.put("host", "user-host")
    userConnectionConfig.put("username", "user")
    val userAdvanced = userConnectionConfig.putObject("advanced")
    userAdvanced.put("ssl", false)
    userAdvanced.put("compression", true)

    // Combine the configs
    val result = handler.combineProperties(templateConfig, userConfig)

    // Assert the results - note that result is the merged connectionConfiguration content
    Assertions.assertNotNull(result, "result should not be null")

    // User values should override template values for conflicting keys
    Assertions.assertEquals("user-host", result.get("host").asText())
    Assertions.assertEquals("user", result.get("username").asText())
    Assertions.assertEquals(false, result.get("advanced").get("ssl").asBoolean())

    // Template values should be preserved for non-conflicting keys
    Assertions.assertEquals(5432, result.get("port").asInt())
    Assertions.assertEquals("template-password", result.get("password").asText())
    Assertions.assertEquals(30, result.get("advanced").get("timeout").asInt())

    // New values from user config should be added
    Assertions.assertEquals(true, result.get("advanced").get("compression").asBoolean())
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
        userConfigSpec = objectMapper.createObjectNode(),
        createdAt = null,
        updatedAt = null,
      ),
      actorName = "test-source",
      actorIcon = "test-icon",
    )

  private fun createMockPartialUserConfigCreate(
    workspaceId: UUID,
    configTemplateId: UUID,
    userConfig: JsonNode = objectMapper.createObjectNode(),
  ): PartialUserConfig =
    PartialUserConfig(
      workspaceId = workspaceId,
      configTemplateId = configTemplateId,
      partialUserConfigProperties = userConfig,
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
          partialUserConfigProperties = properties,
          sourceId = sourceId,
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
        userConfigSpec = objectMapper.createObjectNode(),
      ),
  ): PartialUserConfigWithConfigTemplateAndActorDetails =
    PartialUserConfigWithConfigTemplateAndActorDetails(
      partialUserConfig =
        PartialUserConfig(
          id = id,
          workspaceId = workspaceId,
          configTemplateId = configTemplateId,
          partialUserConfigProperties = properties,
          sourceId = sourceId,
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
