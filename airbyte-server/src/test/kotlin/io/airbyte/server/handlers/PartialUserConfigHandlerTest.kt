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

  @Test
  fun `test combineProperties merges flat configurations correctly`() {
    // Create the template config (just the configuration content)
    val templateConfig = objectMapper.createObjectNode()
    templateConfig.put("host", "template-host")
    templateConfig.put("port", 5432)
    templateConfig.put("database", "template-db")

    // Create the user config (just the configuration content)
    val userConfig = objectMapper.createObjectNode()
    userConfig.put("host", "user-host")
    userConfig.put("username", "user")
    userConfig.put("password", "secret")

    // Combine the configs
    val result = handler.combineProperties(templateConfig, userConfig)

    // Assert the results
    Assertions.assertNotNull(result, "result should not be null")
    Assertions.assertEquals("user-host", result.get("host").asText())
    Assertions.assertEquals(5432, result.get("port").asInt())
    Assertions.assertEquals("template-db", result.get("database").asText())
    Assertions.assertEquals("user", result.get("username").asText())
    Assertions.assertEquals("secret", result.get("password").asText())
  }

  @Test
  fun `test combineProperties merges nested configurations correctly`() {
    // Create the template config with nested properties
    val templateConfig = objectMapper.createObjectNode()
    templateConfig.put("simple", "template-value")
    val templateNestedObj = templateConfig.putObject("nested")
    templateNestedObj.put("prop1", "template-prop1")
    templateNestedObj.put("prop2", "template-prop2")
    val templateDeepNested = templateNestedObj.putObject("deeper")
    templateDeepNested.put("deepProp", "template-deep")

    // Create the user config with nested properties
    val userConfig = objectMapper.createObjectNode()
    userConfig.put("simple", "user-value")
    val userNestedObj = userConfig.putObject("nested")
    userNestedObj.put("prop1", "user-prop1")
    userNestedObj.putArray("newArray").add("item1").add("item2")
    val userDeepNested = userNestedObj.putObject("deeper")
    userDeepNested.put("deepProp", "user-deep")
    userDeepNested.put("newDeepProp", "new-value")

    // Combine the configs
    val result = handler.combineProperties(templateConfig, userConfig)

    // Assert the results
    Assertions.assertNotNull(result, "result should not be null")
    Assertions.assertEquals("user-value", result.get("simple").asText())

    val resultNested = result.get("nested")
    Assertions.assertNotNull(resultNested)
    Assertions.assertTrue(resultNested.isObject)
    Assertions.assertEquals("user-prop1", resultNested.get("prop1").asText())
    Assertions.assertEquals("template-prop2", resultNested.get("prop2").asText())

    val resultArray = resultNested.get("newArray")
    Assertions.assertNotNull(resultArray)
    Assertions.assertTrue(resultArray.isArray)
    Assertions.assertEquals(2, resultArray.size())

    val resultDeeper = resultNested.get("deeper")
    Assertions.assertNotNull(resultDeeper)
    Assertions.assertEquals("user-deep", resultDeeper.get("deepProp").asText())
    Assertions.assertEquals("new-value", resultDeeper.get("newDeepProp").asText())
  }

  @Test
  fun `test combineProperties handles empty configurations`() {
    // Create empty objects for both configs
    val templateConfig = objectMapper.createObjectNode()
    val userConfig = objectMapper.createObjectNode()

    // Combine the configs
    val result = handler.combineProperties(templateConfig, userConfig)

    // Assert the result is an empty object
    Assertions.assertNotNull(result, "result should not be null")
    Assertions.assertTrue(result.isObject)
    Assertions.assertEquals(0, result.size())
  }

  @Test
  fun `test combineProperties handles null configurations`() {
    // Test with null template config
    val userConfig = objectMapper.createObjectNode()
    userConfig.put("key", "value")

    val resultWithNullTemplate = handler.combineProperties(null, userConfig)
    Assertions.assertNotNull(resultWithNullTemplate)
    Assertions.assertEquals("value", resultWithNullTemplate.get("key").asText())

    // Test with null user config
    val templateConfig = objectMapper.createObjectNode()
    templateConfig.put("key", "template-value")

    val resultWithNullUser = handler.combineProperties(templateConfig, null)
    Assertions.assertNotNull(resultWithNullUser)
    Assertions.assertEquals("template-value", resultWithNullUser.get("key").asText())
  }

  @Test
  fun `test combineProperties preserves non-overlapping nested objects`() {
    // Template config with a nested object
    val templateConfig = objectMapper.createObjectNode()
    val templateNestedObj = templateConfig.putObject("templateOnly")
    templateNestedObj.put("prop1", "value1")
    templateNestedObj.put("prop2", "value2")

    // User config with a different nested object
    val userConfig = objectMapper.createObjectNode()
    val userNestedObj = userConfig.putObject("userOnly")
    userNestedObj.put("prop3", "value3")
    userNestedObj.put("prop4", "value4")

    // Combine the configs
    val result = handler.combineProperties(templateConfig, userConfig)

    // Assert both nested objects are preserved
    Assertions.assertTrue(result.has("templateOnly"))
    Assertions.assertTrue(result.has("userOnly"))

    val templateOnlyResult = result.get("templateOnly")
    Assertions.assertEquals("value1", templateOnlyResult.get("prop1").asText())
    Assertions.assertEquals("value2", templateOnlyResult.get("prop2").asText())

    val userOnlyResult = result.get("userOnly")
    Assertions.assertEquals("value3", userOnlyResult.get("prop3").asText())
    Assertions.assertEquals("value4", userOnlyResult.get("prop4").asText())
  }

  @Test
  fun `test combineProperties gives precedence to user config for conflicting keys`() {
    // Template config with some values
    val templateConfig = objectMapper.createObjectNode()
    templateConfig.put("string", "template-string")
    templateConfig.put("number", 100)
    templateConfig.put("boolean", false)
    val templateNestedObj = templateConfig.putObject("nested")
    templateNestedObj.put("sharedProp", "template-value")

    // User config with overlapping values
    val userConfig = objectMapper.createObjectNode()
    userConfig.put("string", "user-string")
    userConfig.put("number", 200)
    userConfig.put("boolean", true)
    val userNestedObj = userConfig.putObject("nested")
    userNestedObj.put("sharedProp", "user-value")

    // Combine the configs
    val result = handler.combineProperties(templateConfig, userConfig)

    // Assert user values take precedence
    Assertions.assertEquals("user-string", result.get("string").asText())
    Assertions.assertEquals(200, result.get("number").asInt())
    Assertions.assertEquals(true, result.get("boolean").asBoolean())

    val nestedResult = result.get("nested")
    Assertions.assertEquals("user-value", nestedResult.get("sharedProp").asText())
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
