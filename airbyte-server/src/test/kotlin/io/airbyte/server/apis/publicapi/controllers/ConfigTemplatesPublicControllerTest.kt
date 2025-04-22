/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.commons.server.authorization.ApiAuthorizationHelper
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.ConfigTemplate
import io.airbyte.config.ConfigTemplateWithActorDetails
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.domain.models.ActorDefinitionId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.publicApi.server.generated.models.ConfigTemplateCreateRequestBody
import io.airbyte.publicApi.server.generated.models.ConfigTemplateUpdateRequestBody
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

@MicronautTest(environments = ["test"])
class ConfigTemplatesPublicControllerTest {
  val organizationId = UUID.randomUUID()

  private val objectMapper: ObjectMapper = ObjectMapper()

  private val configTemplateService: ConfigTemplateService = mockk()
  private val currentUserService: CurrentUserService = mockk()
  private val apiAuthorizationHelper: ApiAuthorizationHelper = mockk()
  private val trackingHelper: TrackingHelper = mockk()
  private val licenseEntitlementChecker: LicenseEntitlementChecker = mockk()
  private val controller =
    ConfigTemplatesPublicController(currentUserService, configTemplateService, apiAuthorizationHelper, trackingHelper, licenseEntitlementChecker)

  @BeforeEach
  fun setup() {
    every { currentUserService.currentUser } returns AuthenticatedUser()
    every { currentUserService.currentUser.userId } returns UUID.randomUUID()
    every { apiAuthorizationHelper.isUserOrganizationAdminOrThrow(any(), any()) } returns Unit
    every { licenseEntitlementChecker.ensureEntitled(any(), any()) } returns Unit
  }

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

    val response = controller.listConfigTemplate(organizationId.toString())

    assertEquals(1, response.data.size)

    assertEquals(configTemplates[0].configTemplate.id, response.data[0].id)
    assertEquals(configTemplates[0].actorName, response.data[0].name)
    assertEquals(configTemplates[0].actorIcon, response.data[0].icon)
  }

  @Test
  fun `test list endpoint returns an object with an empty configTemplates list`() {
    val configTemplates = listOf<ConfigTemplateWithActorDetails>()
    every {
      configTemplateService.listConfigTemplatesForOrganization(OrganizationId(organizationId))
    } returns configTemplates

    val response = controller.listConfigTemplate(organizationId.toString())

    assertTrue(response.data.isEmpty())
  }

  @Test
  fun `test get endpoint`() {
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

    val response = controller.getConfigTemplate(configTemplateId)

    assertEquals(response.id, configTemplateId)
    assertEquals(response.name, configTemplate.actorName)
    assertEquals(response.icon, configTemplate.actorIcon)
    assertEquals(response.sourceDefinitionId, configTemplate.configTemplate.actorDefinitionId)
    assertEquals(response.configTemplateSpec, configTemplate.configTemplate.userConfigSpec)
    assertEquals(response.partialDefaultConfig, configTemplate.configTemplate.partialDefaultConfig)
  }

  @Test
  fun `test create endpoint`() {
    val configTemplateId = UUID.randomUUID()
    val actorDefinitionId = UUID.randomUUID()

    val partialDefaultConfig = objectMapper.readTree("{}")
    val userConfigSpec = objectMapper.readTree("{}")
    val configTemplate =
      ConfigTemplateWithActorDetails(
        ConfigTemplate(
          id = configTemplateId,
          organizationId = organizationId,
          actorDefinitionId = UUID.randomUUID(),
          partialDefaultConfig = partialDefaultConfig,
          userConfigSpec = userConfigSpec,
          createdAt = OffsetDateTime.now(),
          updatedAt = OffsetDateTime.now(),
        ),
        actorName = "actorName",
        actorIcon = "actorIcon",
      )
    every {
      configTemplateService.createTemplate(OrganizationId(organizationId), ActorDefinitionId(actorDefinitionId), partialDefaultConfig, userConfigSpec)
    } returns configTemplate

    val requestBody =
      ConfigTemplateCreateRequestBody(
        organizationId,
        actorDefinitionId,
        partialDefaultConfig,
        userConfigSpec,
      )

    val response = controller.createConfigTemplate(requestBody)

    assertEquals(response.id, configTemplateId)
  }

  @Test
  fun `test update endpoint`() {
    val configTemplateId = UUID.randomUUID()

    val partialDefaultConfig = objectMapper.readTree("{}")
    val userConfigSpec = objectMapper.readTree("{}")
    val configTemplate =
      ConfigTemplateWithActorDetails(
        ConfigTemplate(
          id = configTemplateId,
          organizationId = organizationId,
          actorDefinitionId = UUID.randomUUID(),
          partialDefaultConfig = partialDefaultConfig,
          userConfigSpec = userConfigSpec,
          createdAt = OffsetDateTime.now(),
          updatedAt = OffsetDateTime.now(),
        ),
        actorName = "actorName",
        actorIcon = "actorIcon",
      )
    every {
      configTemplateService.updateTemplate(configTemplateId, OrganizationId(organizationId), partialDefaultConfig, userConfigSpec)
    } returns configTemplate

    val requestBody =
      ConfigTemplateUpdateRequestBody(
        organizationId,
        partialDefaultConfig,
        userConfigSpec,
      )

    val response = controller.updateConfigTemplate(configTemplateId, requestBody)

    assertEquals(response.id, configTemplateId)
  }
}
