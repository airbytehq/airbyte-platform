/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.OrganizationRead
import io.airbyte.api.model.generated.OrganizationReadList
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.commons.server.handlers.OrganizationsHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.ConfigTemplate
import io.airbyte.config.ConfigTemplateWithActorDetails
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.domain.models.ActorDefinitionId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.UseSonarServer
import io.airbyte.protocol.models.v0.ConnectorSpecification
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
  val organizationId: UUID = UUID.randomUUID()

  private val objectMapper: ObjectMapper = ObjectMapper()

  private val configTemplateService: ConfigTemplateService = mockk()
  private val currentUserService: CurrentUserService = mockk()
  private val trackingHelper: TrackingHelper = mockk()
  private val licenseEntitlementChecker: LicenseEntitlementChecker = mockk()
  private val featureFlagClient =
    mockk<FeatureFlagClient> {
      every { boolVariation(UseSonarServer, Organization(organizationId)) } returns false
    }
  private val organizationHandler: OrganizationsHandler = mockk()
  private val controller =
    ConfigTemplatesPublicController(
      currentUserService,
      configTemplateService,
      trackingHelper,
      licenseEntitlementChecker,
      organizationHandler,
      featureFlagClient,
    )
  private val organizationReadList =
    OrganizationReadList().organizations(listOf(OrganizationRead().organizationId(organizationId)))

  @BeforeEach
  fun setup() {
    every { currentUserService.getCurrentUser() } returns AuthenticatedUser()
    every { currentUserService.getCurrentUser().userId } returns UUID.randomUUID()
    every { licenseEntitlementChecker.ensureEntitled(any(), any()) } returns Unit
    every { licenseEntitlementChecker.ensureEntitled(any(), any(), any()) } returns Unit
    every { organizationHandler.listOrganizationsByUser(any()) } returns organizationReadList
    every { licenseEntitlementChecker.checkEntitlements(any(), any()) } returns true
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
          userConfigSpec = ConnectorSpecification().withConnectionSpecification(objectMapper.readTree("{}")),
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
          userConfigSpec = ConnectorSpecification().withConnectionSpecification(objectMapper.readTree("{}")),
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
  fun `test create endpoint without a user spec`() {
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
          userConfigSpec = ConnectorSpecification().withConnectionSpecification(objectMapper.readTree("{}")),
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
          userConfigSpec = ConnectorSpecification().withConnectionSpecification(objectMapper.readTree("{}")),
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
