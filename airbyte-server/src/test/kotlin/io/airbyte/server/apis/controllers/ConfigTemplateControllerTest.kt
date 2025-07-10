/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.ConfigTemplateRequestBody
import io.airbyte.api.model.generated.ListConfigTemplatesRequestBody
import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.config.ConfigTemplate
import io.airbyte.config.ConfigTemplateWithActorDetails
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.domain.models.OrganizationId
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.UseSonarServer
import io.airbyte.persistence.job.WorkspaceHelper
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

@MicronautTest
class ConfigTemplateControllerTest {
  val organizationId = UUID.randomUUID()
  val workspaceId = UUID.randomUUID()

  val objectMapper: ObjectMapper = ObjectMapper()

  val configTemplateService: ConfigTemplateService = mockk()
  val workspaceHelper: WorkspaceHelper =
    mockk {
      every {
        getOrganizationForWorkspace(workspaceId)
      } returns organizationId
    }
  private val licenseEntitlementChecker: LicenseEntitlementChecker =
    mockk {
      every {
        ensureEntitled(organizationId, Entitlement.CONFIG_TEMPLATE_ENDPOINTS)
      } returns Unit
    }

  private val featureFlagClient =
    mockk<FeatureFlagClient> {
      every { boolVariation(UseSonarServer, Organization(organizationId)) } returns false
    }

  val controller = ConfigTemplateController(configTemplateService, workspaceHelper, licenseEntitlementChecker, featureFlagClient)

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
                userConfigSpec = ConnectorSpecification().withConnectionSpecification(objectMapper.readTree("{}")),
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
      requestBody.workspaceId = workspaceId

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
      requestBody.workspaceId = workspaceId

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
            userConfigSpec = ConnectorSpecification().withConnectionSpecification(objectMapper.readTree("{}")),
            createdAt = OffsetDateTime.now(),
            updatedAt = OffsetDateTime.now(),
          ),
          actorName = "actorName",
          actorIcon = "actorIcon",
        )
      every {
        configTemplateService.getConfigTemplate(configTemplateId, workspaceId)
      } returns configTemplate

      val requestBody = ConfigTemplateRequestBody()

      requestBody.configTemplateId = configTemplateId
      requestBody.workspaceId = workspaceId

      val response = controller.getConfigTemplate(requestBody)

      assertEquals(response.id, configTemplateId)
      assertEquals(response.name, configTemplate.actorName)
      assertEquals(response.icon, configTemplate.actorIcon)
      assertEquals(response.sourceDefinitionId, configTemplate.configTemplate.actorDefinitionId)
      assertEquals(
        response.configTemplateSpec,
        configTemplate.configTemplate.userConfigSpec.let {
          objectMapper.valueToTree<JsonNode>(it)
        },
      )
    }
  }
}
