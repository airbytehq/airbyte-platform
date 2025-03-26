/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.PartialUserConfigCreate
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.data.repositories.entities.ConfigTemplate
import io.airbyte.data.repositories.entities.PartialUserConfig
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.data.services.PartialUserConfigService
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

class PartialUserConfigHandlerTest {
  private lateinit var partialUserConfigService: PartialUserConfigService
  private lateinit var configTemplateService: ConfigTemplateService
  private lateinit var actorDefinitionService: ActorDefinitionService
  private lateinit var sourceHandler: SourceHandler
  private lateinit var handler: PartialUserConfigHandler
  private lateinit var objectMapper: ObjectMapper

  @BeforeEach
  fun setup() {
    partialUserConfigService = mockk<PartialUserConfigService>()
    configTemplateService = mockk<ConfigTemplateService>()
    actorDefinitionService = mockk<ActorDefinitionService>()
    sourceHandler = mockk<SourceHandler>()
    handler = PartialUserConfigHandler(partialUserConfigService, configTemplateService, actorDefinitionService, sourceHandler)
    objectMapper = ObjectMapper()
  }

  @Test
  fun `test createPartialUserConfig with valid inputs`() {
    val workspaceId = UUID.randomUUID()
    val configTemplateId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val partialUserConfigId = UUID.randomUUID()

    val configTemplate = createMockConfigTemplate(configTemplateId)
    val actorDefinition = createMockActorDefinition(configTemplate.actorDefinitionId)
    val partialUserConfigCreate = createMockPartialUserConfigCreate(workspaceId, configTemplateId)
    val savedPartialUserConfig = createMockPartialUserConfig(partialUserConfigId, workspaceId, configTemplateId)
    val savedSource = createMockSourceRead(sourceId)

    every { configTemplateService.getConfigTemplate(configTemplateId) } returns configTemplate
    every { partialUserConfigService.createPartialUserConfig(any()) } returns savedPartialUserConfig
    every { sourceHandler.createSource(any()) } returns savedSource
    every { actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(configTemplate.actorDefinitionId) } returns
      Optional.of(actorDefinition)

    val result = handler.createPartialUserConfig(partialUserConfigCreate)

    Assertions.assertNotNull(result)
    Assertions.assertEquals(partialUserConfigId, result.partialUserConfig.partialUserConfigId)
    Assertions.assertEquals(sourceId, result.partialUserConfig.sourceId)
    Assertions.assertEquals(sourceId, result.source.sourceId)

    verify { partialUserConfigService.createPartialUserConfig(any()) }
    verify { sourceHandler.createSource(any()) }
  }

  @Test
  fun `test combineProperties merges flat configurations correctly`() {
    val templateConfig =
      objectMapper.createObjectNode().apply {
        putObject("connectionConfiguration").apply {
          put("host", "template-host")
          put("port", 5432)
          put("database", "template-db")
        }
      }

    val userConfig =
      objectMapper.createObjectNode().apply {
        putObject("connectionConfiguration").apply {
          put("host", "user-host")
          put("username", "user")
          put("password", "secret")
        }
      }

    val result = handler.combineProperties(templateConfig, userConfig)

    Assertions.assertEquals("user-host", result.get("host").asText())
    Assertions.assertEquals(5432, result.get("port").asInt())
    Assertions.assertEquals("template-db", result.get("database").asText())
    Assertions.assertEquals("user", result.get("username").asText())
    Assertions.assertEquals("secret", result.get("password").asText())
  }

  @Test
  fun `test combineProperties merges nested configurations correctly`() {
    val templateConfig =
      objectMapper.createObjectNode().apply {
        putObject("connectionConfiguration").apply {
          put("host", "template-host")
          putObject("credentials").apply {
            put("type", "password")
            put("username", "template-user")
          }
        }
      }

    val userConfig =
      objectMapper.createObjectNode().apply {
        putObject("connectionConfiguration").apply {
          put("host", "user-host")
          putObject("credentials").apply {
            put("username", "user")
            put("password", "secret")
          }
        }
      }

    val result = handler.combineProperties(templateConfig, userConfig)

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
    val templateConfig =
      objectMapper.createObjectNode().apply {
        putObject("connectionConfiguration").apply {
          putObject("source").apply {
            put("type", "postgres")
            put("host", "template-host")
          }
        }
      }

    val userConfig =
      objectMapper.createObjectNode().apply {
        putObject("connectionConfiguration").apply {
          putObject("destination").apply {
            put("type", "snowflake")
            put("account", "user-account")
          }
        }
      }

    val result = handler.combineProperties(templateConfig, userConfig)

    Assertions.assertEquals("postgres", result.get("source").get("type").asText())
    Assertions.assertEquals("template-host", result.get("source").get("host").asText())
    Assertions.assertEquals("snowflake", result.get("destination").get("type").asText())
    Assertions.assertEquals("user-account", result.get("destination").get("account").asText())
  }

  private fun createMockConfigTemplate(
    id: UUID,
    partialDefaultConfig: JsonNode = objectMapper.createObjectNode(),
  ): ConfigTemplate =
    ConfigTemplate(
      id = id,
      actorDefinitionId = UUID.randomUUID(),
      partialDefaultConfig = partialDefaultConfig,
      organizationId = UUID.randomUUID(),
      userConfigSpec = objectMapper.createObjectNode(),
    )

  private fun createMockActorDefinition(id: UUID): ActorDefinitionVersion {
    val actorDefinitionVersion = ActorDefinitionVersion()
    val actorDefinitionIdField = ActorDefinitionVersion::class.java.getDeclaredField("actorDefinitionId")
    actorDefinitionIdField.isAccessible = true
    actorDefinitionIdField.set(actorDefinitionVersion, id)

    return actorDefinitionVersion
  }

  private fun createMockPartialUserConfigCreate(
    workspaceId: UUID,
    configTemplateId: UUID,
    userConfig: JsonNode = objectMapper.createObjectNode(),
  ): PartialUserConfigCreate =
    PartialUserConfigCreate().apply {
      this.workspaceId = workspaceId
      this.configTemplateId = configTemplateId
      this.partialUserConfigProperties = userConfig
    }

  private fun createMockPartialUserConfig(
    id: UUID,
    workspaceId: UUID,
    configTemplateId: UUID,
  ): PartialUserConfig =
    PartialUserConfig(
      id = id,
      workspaceId = workspaceId,
      configTemplateId = configTemplateId,
      partialUserConfigProperties = objectMapper.createObjectNode(),
      tombstone = false,
    )

  private fun createMockSourceRead(id: UUID): SourceRead =
    SourceRead().apply {
      sourceId = id
      name = "test-source"
      sourceDefinitionId = UUID.randomUUID()
      connectionConfiguration = objectMapper.createObjectNode()
    }
}
