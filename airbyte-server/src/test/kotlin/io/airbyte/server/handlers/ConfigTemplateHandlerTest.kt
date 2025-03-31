/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.data.repositories.entities.ConfigTemplate
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.data.services.SourceService
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class ConfigTemplateHandlerTest {
  private lateinit var configTemplateService: ConfigTemplateService
  private lateinit var sourceService: SourceService
  private lateinit var handler: ConfigTemplateHandler
  private val objectMapper = ObjectMapper()

  // Common test data
  private val actorDefinitionId = UUID.randomUUID()
  private val organizationId = UUID.randomUUID()
  private val configTemplateId = UUID.randomUUID()
  private val partialDefaultConfig: JsonNode = objectMapper.readTree("{}")
  private val userConfigSpec = objectMapper.readTree("{}")
  private val sourceDefinition =
    StandardSourceDefinition().apply {
      name = "test-source"
      iconUrl = "test-icon"
      sourceDefinitionId = actorDefinitionId
    }

  @BeforeEach
  fun setUp() {
    configTemplateService = mockk<ConfigTemplateService>()
    sourceService = mockk<SourceService>()
    handler = ConfigTemplateHandler(configTemplateService, sourceService)

    // Common mock setup
    every { sourceService.getStandardSourceDefinition(actorDefinitionId, false) } returns sourceDefinition
  }

  /**
   * Helper function to create a ConfigTemplate for testing
   */
  private fun createConfigTemplate(
    id: UUID = configTemplateId,
    orgId: UUID = organizationId,
    actorDefId: UUID = actorDefinitionId,
    defaultConfig: JsonNode = partialDefaultConfig,
    configSpec: JsonNode = userConfigSpec,
  ): ConfigTemplate =
    ConfigTemplate(
      id = id,
      organizationId = orgId,
      actorDefinitionId = actorDefId,
      partialDefaultConfig = defaultConfig,
      userConfigSpec = configSpec,
    )

  @Test
  fun testListConfigTemplatesForOrganization() {
    val configTemplate = createConfigTemplate()
    every { configTemplateService.listConfigTemplates(organizationId) } returns listOf(configTemplate)

    val result = handler.listConfigTemplatesForOrganization(organizationId)

    assertEquals(1, result.size)
    assertEquals(configTemplateId, result[0].configTemplate.id)
    assertEquals("test-source", result[0].actorName)
    assertEquals("test-icon", result[0].actorIcon)

    verify { configTemplateService.listConfigTemplates(organizationId) }
  }

  @Test
  fun testGetConfigTemplate() {
    val configTemplate = createConfigTemplate()
    every { configTemplateService.getConfigTemplate(configTemplateId) } returns configTemplate

    val result = handler.getConfigTemplate(configTemplateId)

    assertNotNull(result)
    assertEquals(configTemplateId, result.configTemplate.id)
    assertEquals("test-source", result.actorName)
    assertEquals("test-icon", result.actorIcon)
    assertEquals(objectMapper.readTree("{}"), result.configTemplate.userConfigSpec)

    verify { configTemplateService.getConfigTemplate(configTemplateId) }
  }
}
