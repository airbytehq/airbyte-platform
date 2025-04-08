/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.ConfigTemplateRequestBody
import io.airbyte.api.model.generated.ListConfigTemplatesRequestBody
import io.airbyte.config.ConfigTemplate
import io.airbyte.config.ConfigTemplateWithActorDetails
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.domain.models.OrganizationId
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

@MicronautTest(environments = ["test"])
class ConfigTemplateControllerTest {
  val organizationId = UUID.randomUUID()

  private val objectMapper: ObjectMapper = ObjectMapper()

  private val configTemplateService: ConfigTemplateService = mockk()
  private val controller = ConfigTemplateController(configTemplateService)

  @Nested
  inner class ListEndpointTests {
    @Test
    fun `test list endpoint`() {
      val configTemplates =
        listOf(
          ConfigTemplateWithActorDetails(
            configTemplate =
              ConfigTemplate(
                id = UUID.randomUUID(),
                organizationId = organizationId,
                actorDefinitionId = UUID.randomUUID(),
                partialDefaultConfig = objectMapper.readTree("{}"),
                userConfigSpec = objectMapper.readTree("{}"),
                createdAt = OffsetDateTime.now(),
                updatedAt = OffsetDateTime.now(),
              ),
            actorName = "actorName",
            actorIcon = "actorIcon",
          ),
        )
      every {
        configTemplateService.listConfigTemplatesForOrganization(OrganizationId(organizationId))
      } returns configTemplates

      val requestBody = ListConfigTemplatesRequestBody()
      requestBody.organizationId = organizationId

      val response = controller.listConfigTemplates(requestBody)

      assertEquals(1, response.configTemplates.size)

      assertEquals(configTemplates[0].configTemplate.id, response.configTemplates[0].id)
      assertEquals(configTemplates[0].actorName, response.configTemplates[0].name)
      assertEquals(configTemplates[0].actorIcon, response.configTemplates[0].icon)
    }

    @Test
    fun `test list endpoint returns an object with an empty configTemplates list`() {
      val configTemplates = listOf<ConfigTemplateWithActorDetails>()
      every {
        configTemplateService.listConfigTemplatesForOrganization(OrganizationId(organizationId))
      } returns configTemplates

      val requestBody = ListConfigTemplatesRequestBody()
      requestBody.organizationId = organizationId

      val response = controller.listConfigTemplates(requestBody)

      assertTrue(response.configTemplates.isEmpty())
    }
  }

  @Nested
  inner class GetEndpointTest {
    @Test
    fun test() {
      val configTemplateId = UUID.randomUUID()

      val configTemplate =
        ConfigTemplateWithActorDetails(
          ConfigTemplate(
            id = configTemplateId,
            organizationId = organizationId,
            actorDefinitionId = UUID.randomUUID(),
            partialDefaultConfig = objectMapper.readTree("{}"),
            userConfigSpec = objectMapper.readTree("{}"),
            createdAt = OffsetDateTime.now(),
            updatedAt = OffsetDateTime.now(),
          ),
          actorName = "actorName",
          actorIcon = "actorIcon",
        )
      every {
        configTemplateService.getConfigTemplate(configTemplateId)
      } returns configTemplate

      val requestBody = ConfigTemplateRequestBody()

      requestBody.configTemplateId = configTemplateId

      val response = controller.getConfigTemplate(requestBody)

      assertEquals(response.id, configTemplateId)
      assertEquals(response.name, configTemplate.actorName)
      assertEquals(response.icon, configTemplate.actorIcon)
      assertEquals(response.sourceDefinitionId, configTemplate.configTemplate.actorDefinitionId)
      assertEquals(response.configTemplateSpec, configTemplate.configTemplate.userConfigSpec)
    }
  }
}
