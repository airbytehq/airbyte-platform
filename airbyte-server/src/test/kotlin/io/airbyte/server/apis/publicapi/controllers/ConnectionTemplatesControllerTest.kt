/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.commons.constants.GEOGRAPHY_AUTO
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.commons.server.authorization.ApiAuthorizationHelper
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.ConnectionTemplate
import io.airbyte.config.Cron
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType
import io.airbyte.config.ScheduleData
import io.airbyte.config.StandardSync.NonBreakingChangesPreference
import io.airbyte.config.StandardSync.ScheduleType
import io.airbyte.data.services.ActorDefinitionIdOrType
import io.airbyte.data.services.ConnectionTemplateService
import io.airbyte.domain.models.ActorDefinitionId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.publicApi.server.generated.models.AirbyteApiConnectionSchedule
import io.airbyte.publicApi.server.generated.models.ConnectionTemplateCreateRequestBody
import io.airbyte.publicApi.server.generated.models.ScheduleTypeEnum
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.controllers.ConnectionTemplatesController.Companion.DEFAULT_CRON_SCHEDULE
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

@MicronautTest(environments = ["test"])
class ConnectionTemplatesControllerTest {
  private val objectMapper: ObjectMapper = ObjectMapper()

  private val currentUserService: CurrentUserService = mockk()
  private val apiAuthorizationHelper: ApiAuthorizationHelper = mockk()
  private val trackingHelper: TrackingHelper = mockk()
  private val licenseEntitlementChecker: LicenseEntitlementChecker = mockk()
  private val connectionTemplateService: ConnectionTemplateService = mockk()
  private val controller =
    ConnectionTemplatesController(currentUserService, apiAuthorizationHelper, trackingHelper, licenseEntitlementChecker, connectionTemplateService)

  val destinationName = "destination_name"
  val actorDefinitionId = ActorDefinitionId(UUID.randomUUID())
  val destinationType = "s3"
  val destinationConfig = objectMapper.readTree("{}")
  val organizationId = OrganizationId(UUID.randomUUID())
  val namespaceDefinitionType = NamespaceDefinitionType.DESTINATION
  val defaultNamespaceDefinitionType = NamespaceDefinitionType.CUSTOMFORMAT
  val namespaceFormat = "format"
  val prefix = "prefix_"
  val cronExpression = "0 1 * * * ?"
  val cronScheduleData = ScheduleData().withCron(Cron().withCronExpression(cronExpression).withCronTimeZone("UTC"))
  val resourceRequirements = null
  val ignoreNonBreakingChangesPreference = NonBreakingChangesPreference.IGNORE
  val defaultGeography = GEOGRAPHY_AUTO

  @BeforeEach
  fun setup() {
    every { currentUserService.currentUser } returns AuthenticatedUser()
    every { currentUserService.currentUser.userId } returns UUID.randomUUID()
    every { apiAuthorizationHelper.isUserOrganizationAdminOrThrow(any(), any()) } returns Unit
    every { licenseEntitlementChecker.ensureEntitled(any(), any()) } returns Unit
  }

  @Test
  fun `test create endpoint with destination actor definition ID`() {
    val connectionTemplate =
      ConnectionTemplate(
        id = UUID.randomUUID(),
        destinationName = destinationName,
        organizationId = organizationId,
        destinationActorDefinitionId = actorDefinitionId.value,
        destinationConfiguration = destinationConfig,
        namespaceDefinitionType = NamespaceDefinitionType.CUSTOMFORMAT,
        namespaceFormat = null,
        prefix = prefix,
        scheduleType = ScheduleType.CRON,
        scheduleData = DEFAULT_CRON_SCHEDULE,
        resourceRequirements = resourceRequirements,
        nonBreakingChangesPreference = ignoreNonBreakingChangesPreference,
        defaultGeography = defaultGeography,
        syncOnCreate = true,
      )

    every {
      connectionTemplateService.createTemplate(
        any(),
        eq(destinationName),
        eq(ActorDefinitionIdOrType.DefinitionId(actorDefinitionId)),
        eq(destinationConfig),
        eq(NamespaceDefinitionType.CUSTOMFORMAT),
        isNull(),
        prefix,
        DEFAULT_CRON_SCHEDULE,
        isNull(),
        ignoreNonBreakingChangesPreference,
        isNull(),
        true,
      )
    } returns connectionTemplate

    val requestBody =
      ConnectionTemplateCreateRequestBody(
        organizationId.value,
        destinationName,
        destinationConfig,
        null,
        actorDefinitionId.value,
        prefix = prefix,
      )

    val response = controller.createConnectionTemplate(requestBody)

    assertEquals(response.id, connectionTemplate.id)
  }

  @Test
  fun `test create endpoint with destination type`() {
    val connectionTemplate =
      ConnectionTemplate(
        id = UUID.randomUUID(),
        organizationId = organizationId,
        destinationName = destinationName,
        destinationActorDefinitionId = actorDefinitionId.value,
        destinationConfiguration = destinationConfig,
        namespaceDefinitionType = namespaceDefinitionType,
        namespaceFormat = namespaceFormat,
        prefix = prefix,
        scheduleType = ScheduleType.CRON,
        scheduleData = DEFAULT_CRON_SCHEDULE,
        resourceRequirements = resourceRequirements,
        nonBreakingChangesPreference = ignoreNonBreakingChangesPreference,
        defaultGeography = defaultGeography,
        syncOnCreate = false,
      )

    every {
      connectionTemplateService.createTemplate(
        any(),
        eq(destinationName),
        eq(ActorDefinitionIdOrType.Type(destinationType)),
        eq(destinationConfig),
        namespaceDefinitionType,
        namespaceFormat,
        prefix,
        DEFAULT_CRON_SCHEDULE,
        isNull(),
        ignoreNonBreakingChangesPreference,
        defaultGeography,
        false,
      )
    } returns connectionTemplate

    val requestBody =
      ConnectionTemplateCreateRequestBody(
        organizationId.value,
        destinationName,
        destinationConfig,
        destinationType,
        null,
        io.airbyte.publicApi.server.generated.models.NamespaceDefinitionType.DESTINATION,
        namespaceFormat,
        prefix,
        defaultGeography = defaultGeography,
        syncOnCreate = false,
      )

    val response = controller.createConnectionTemplate(requestBody)

    assertEquals(response.id, connectionTemplate.id)
  }

  @Test
  fun `test create endpoint with both destination actor definition ID and actor type`() {
    val requestBody =
      ConnectionTemplateCreateRequestBody(
        organizationId.value,
        destinationType,
        destinationConfig,
        destinationType,
        actorDefinitionId.value,
        io.airbyte.publicApi.server.generated.models.NamespaceDefinitionType.DESTINATION,
        namespaceFormat,
        prefix,
      )

    org.junit.jupiter.api.assertThrows<IllegalArgumentException> {
      controller.createConnectionTemplate(requestBody)
    }
  }

  @Test
  fun `test create endpoint with PROPAGATE_COLUMNS breaking changes preference`() {
    val connectionTemplate =
      ConnectionTemplate(
        id = UUID.randomUUID(),
        organizationId = organizationId,
        destinationName = destinationName,
        destinationActorDefinitionId = actorDefinitionId.value,
        destinationConfiguration = destinationConfig,
        namespaceDefinitionType = NamespaceDefinitionType.SOURCE,
        namespaceFormat = namespaceFormat,
        prefix = prefix,
        scheduleType = ScheduleType.CRON,
        scheduleData = DEFAULT_CRON_SCHEDULE,
        resourceRequirements = resourceRequirements,
        nonBreakingChangesPreference = NonBreakingChangesPreference.PROPAGATE_COLUMNS,
        defaultGeography = defaultGeography,
        syncOnCreate = true,
      )

    every {
      connectionTemplateService.createTemplate(
        any(),
        eq(destinationName),
        eq(ActorDefinitionIdOrType.DefinitionId(actorDefinitionId)),
        eq(destinationConfig),
        NamespaceDefinitionType.SOURCE,
        namespaceFormat,
        prefix,
        DEFAULT_CRON_SCHEDULE,
        isNull(),
        NonBreakingChangesPreference.PROPAGATE_COLUMNS,
        isNull(),
        true,
      )
    } returns connectionTemplate

    val requestBody =
      ConnectionTemplateCreateRequestBody(
        organizationId.value,
        destinationName,
        destinationConfig,
        null,
        actorDefinitionId.value,
        io.airbyte.publicApi.server.generated.models.NamespaceDefinitionType.SOURCE,
        namespaceFormat,
        prefix,
        nonBreakingChangesPreference = io.airbyte.publicApi.server.generated.models.NonBreakingChangesPreference.PROPAGATE_COLUMNS,
        syncOnCreate = true,
      )

    val response = controller.createConnectionTemplate(requestBody)

    assertEquals(response.id, connectionTemplate.id)
  }

  @Test
  fun `test create endpoint with PROPAGATE_FULLY breaking changes preference`() {
    val connectionTemplate =
      ConnectionTemplate(
        id = UUID.randomUUID(),
        organizationId = organizationId,
        destinationName = destinationName,
        destinationActorDefinitionId = actorDefinitionId.value,
        destinationConfiguration = destinationConfig,
        namespaceDefinitionType = NamespaceDefinitionType.CUSTOMFORMAT,
        namespaceFormat = namespaceFormat,
        prefix = prefix,
        scheduleType = ScheduleType.CRON,
        scheduleData = DEFAULT_CRON_SCHEDULE,
        resourceRequirements = resourceRequirements,
        nonBreakingChangesPreference = NonBreakingChangesPreference.PROPAGATE_FULLY,
        defaultGeography = defaultGeography,
        syncOnCreate = true,
      )

    every {
      connectionTemplateService.createTemplate(
        any(),
        eq(destinationName),
        eq(ActorDefinitionIdOrType.DefinitionId(actorDefinitionId)),
        eq(destinationConfig),
        NamespaceDefinitionType.CUSTOMFORMAT,
        namespaceFormat,
        prefix,
        DEFAULT_CRON_SCHEDULE,
        isNull(),
        NonBreakingChangesPreference.PROPAGATE_FULLY,
        isNull(),
        true,
      )
    } returns connectionTemplate

    val requestBody =
      ConnectionTemplateCreateRequestBody(
        organizationId.value,
        destinationName,
        destinationConfig,
        null,
        actorDefinitionId.value,
        io.airbyte.publicApi.server.generated.models.NamespaceDefinitionType.CUSTOMFORMAT,
        namespaceFormat,
        prefix,
        nonBreakingChangesPreference = io.airbyte.publicApi.server.generated.models.NonBreakingChangesPreference.PROPAGATE_FULLY,
      )

    val response = controller.createConnectionTemplate(requestBody)

    assertEquals(response.id, connectionTemplate.id)
  }

  @Test
  fun `test create endpoint with IGNORE breaking changes preference`() {
    val connectionTemplate =
      ConnectionTemplate(
        id = UUID.randomUUID(),
        organizationId = organizationId,
        destinationName = destinationName,
        destinationActorDefinitionId = actorDefinitionId.value,
        destinationConfiguration = destinationConfig,
        namespaceDefinitionType = namespaceDefinitionType,
        namespaceFormat = namespaceFormat,
        prefix = prefix,
        scheduleType = ScheduleType.CRON,
        scheduleData = DEFAULT_CRON_SCHEDULE,
        resourceRequirements = resourceRequirements,
        nonBreakingChangesPreference = ignoreNonBreakingChangesPreference,
        defaultGeography = defaultGeography,
        syncOnCreate = true,
      )

    every {
      connectionTemplateService.createTemplate(
        any(),
        eq(destinationName),
        eq(ActorDefinitionIdOrType.DefinitionId(actorDefinitionId)),
        eq(destinationConfig),
        namespaceDefinitionType,
        namespaceFormat,
        prefix,
        DEFAULT_CRON_SCHEDULE,
        isNull(),
        ignoreNonBreakingChangesPreference,
        isNull(),
        true,
      )
    } returns connectionTemplate

    val requestBody =
      ConnectionTemplateCreateRequestBody(
        organizationId.value,
        destinationName,
        destinationConfig,
        null,
        actorDefinitionId.value,
        io.airbyte.publicApi.server.generated.models.NamespaceDefinitionType.DESTINATION,
        namespaceFormat,
        prefix,
        nonBreakingChangesPreference = io.airbyte.publicApi.server.generated.models.NonBreakingChangesPreference.IGNORE,
      )

    val response = controller.createConnectionTemplate(requestBody)

    assertEquals(response.id, connectionTemplate.id)
  }

  @Test
  fun `test create endpoint with DISABLE breaking changes preference`() {
    val connectionTemplate =
      ConnectionTemplate(
        id = UUID.randomUUID(),
        organizationId = organizationId,
        destinationName = destinationName,
        destinationActorDefinitionId = actorDefinitionId.value,
        destinationConfiguration = destinationConfig,
        namespaceDefinitionType = namespaceDefinitionType,
        namespaceFormat = namespaceFormat,
        prefix = prefix,
        scheduleType = ScheduleType.CRON,
        scheduleData = DEFAULT_CRON_SCHEDULE,
        resourceRequirements = resourceRequirements,
        nonBreakingChangesPreference = NonBreakingChangesPreference.DISABLE,
        defaultGeography = defaultGeography,
        syncOnCreate = true,
      )

    every {
      connectionTemplateService.createTemplate(
        any(),
        eq(destinationName),
        eq(ActorDefinitionIdOrType.DefinitionId(actorDefinitionId)),
        eq(destinationConfig),
        namespaceDefinitionType,
        namespaceFormat,
        prefix,
        DEFAULT_CRON_SCHEDULE,
        isNull(),
        NonBreakingChangesPreference.DISABLE,
        isNull(),
        true,
      )
    } returns connectionTemplate

    val requestBody =
      ConnectionTemplateCreateRequestBody(
        organizationId.value,
        destinationName,
        destinationConfig,
        null,
        actorDefinitionId.value,
        io.airbyte.publicApi.server.generated.models.NamespaceDefinitionType.DESTINATION,
        namespaceFormat,
        prefix,
        nonBreakingChangesPreference = io.airbyte.publicApi.server.generated.models.NonBreakingChangesPreference.DISABLE,
      )

    val response = controller.createConnectionTemplate(requestBody)

    assertEquals(response.id, connectionTemplate.id)
  }

  @Test
  fun `test create endpoint with cron schedule`() {
    val connectionTemplate =
      ConnectionTemplate(
        id = UUID.randomUUID(),
        destinationName = destinationName,
        organizationId = organizationId,
        destinationActorDefinitionId = actorDefinitionId.value,
        destinationConfiguration = destinationConfig,
        namespaceDefinitionType = defaultNamespaceDefinitionType,
        namespaceFormat = null,
        prefix = null,
        scheduleType = ScheduleType.CRON,
        scheduleData = cronScheduleData,
        resourceRequirements = resourceRequirements,
        nonBreakingChangesPreference = ignoreNonBreakingChangesPreference,
        defaultGeography = defaultGeography,
        syncOnCreate = true,
      )

    every {
      connectionTemplateService.createTemplate(
        any(),
        eq(destinationName),
        eq(ActorDefinitionIdOrType.DefinitionId(actorDefinitionId)),
        eq(destinationConfig),
        eq(NamespaceDefinitionType.CUSTOMFORMAT),
        isNull(),
        isNull(),
        eq(cronScheduleData),
        isNull(),
        eq(ignoreNonBreakingChangesPreference),
        isNull(),
        eq(true),
      )
    } returns connectionTemplate

    val requestBody =
      ConnectionTemplateCreateRequestBody(
        organizationId.value,
        destinationName,
        destinationConfig,
        null,
        actorDefinitionId.value,
        schedule = AirbyteApiConnectionSchedule(scheduleType = ScheduleTypeEnum.CRON, cronExpression),
      )

    val response = controller.createConnectionTemplate(requestBody)

    assertEquals(response.id, connectionTemplate.id)
  }

  @Test
  fun `test create endpoint with manual schedule`() {
    val connectionTemplate =
      ConnectionTemplate(
        id = UUID.randomUUID(),
        destinationName = destinationName,
        organizationId = organizationId,
        destinationActorDefinitionId = actorDefinitionId.value,
        destinationConfiguration = destinationConfig,
        namespaceDefinitionType = defaultNamespaceDefinitionType,
        namespaceFormat = null,
        prefix = null,
        scheduleType = ScheduleType.MANUAL,
        scheduleData = null,
        resourceRequirements = resourceRequirements,
        nonBreakingChangesPreference = ignoreNonBreakingChangesPreference,
        defaultGeography = defaultGeography,
        syncOnCreate = true,
      )

    every {
      connectionTemplateService.createTemplate(
        any(),
        eq(destinationName),
        eq(ActorDefinitionIdOrType.DefinitionId(actorDefinitionId)),
        eq(destinationConfig),
        eq(NamespaceDefinitionType.CUSTOMFORMAT),
        isNull(),
        isNull(),
        isNull(),
        isNull(),
        eq(ignoreNonBreakingChangesPreference),
        isNull(),
        eq(true),
      )
    } returns connectionTemplate

    val requestBody =
      ConnectionTemplateCreateRequestBody(
        organizationId.value,
        destinationName,
        destinationConfig,
        null,
        actorDefinitionId.value,
        schedule = AirbyteApiConnectionSchedule(scheduleType = ScheduleTypeEnum.MANUAL),
      )

    val response = controller.createConnectionTemplate(requestBody)

    assertEquals(response.id, connectionTemplate.id)
  }
}
