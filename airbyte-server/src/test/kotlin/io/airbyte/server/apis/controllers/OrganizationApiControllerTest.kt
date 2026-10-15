/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.OrganizationAgenticStatusUpdateRequestBody
import io.airbyte.api.server.generated.models.OrganizationCreateRequestBody
import io.airbyte.api.server.generated.models.OrganizationDataWorkerUsageRequestBody
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.api.server.generated.models.OrganizationInfoRead
import io.airbyte.api.server.generated.models.OrganizationRead
import io.airbyte.api.server.generated.models.OrganizationUpdateRequestBody
import io.airbyte.commons.server.handlers.OrganizationsHandler
import io.airbyte.server.helpers.OrganizationAccessAuthorizationHelper
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.LocalDate
import java.util.UUID

class OrganizationApiControllerTest {
  private lateinit var organizationsHandler: OrganizationsHandler
  private lateinit var organizationAccessAuthorizationHelper: OrganizationAccessAuthorizationHelper

  private lateinit var organizationApiController: OrganizationApiController

  private val organizationId = UUID.randomUUID()
  private val organizationInfoRead =
    OrganizationInfoRead(
      organizationId = organizationId,
      organizationName = "Test Organization",
      sso = false,
      scim = false,
    )

  private val organizationRead = OrganizationRead(organizationId = organizationId, organizationName = "Test Organization", email = "test@example.com")

  @BeforeEach
  fun setup() {
    organizationsHandler = mockk()
    organizationAccessAuthorizationHelper = mockk(relaxed = true)
    organizationApiController =
      OrganizationApiController(organizationsHandler, organizationAccessAuthorizationHelper)

    // Default behavior: organizationsHandler returns organization info
    every { organizationsHandler.getOrganizationInfo(organizationId) } returns organizationInfoRead
  }

  @Test
  fun `getOrganization succeeds when validation passes`() {
    val requestBody = OrganizationIdRequestBody(organizationId = organizationId)
    every { organizationAccessAuthorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId) } returns Unit
    every { organizationsHandler.getOrganization(requestBody) } returns organizationRead

    val result = organizationApiController.getOrganization(requestBody)

    assertNotNull(result)
    verify(exactly = 1) { organizationAccessAuthorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId) }
    verify(exactly = 1) { organizationsHandler.getOrganization(requestBody) }
  }

  @Test
  fun `getOrganization fails when validation fails`() {
    val forbiddenException = ForbiddenProblem()
    every { organizationAccessAuthorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId) } throws forbiddenException

    val requestBody = OrganizationIdRequestBody(organizationId = organizationId)

    val exception =
      assertThrows<ForbiddenProblem> {
        organizationApiController.getOrganization(requestBody)
      }

    assertEquals(forbiddenException, exception)
    verify(exactly = 0) { organizationsHandler.getOrganization(any()) }
    verify(exactly = 1) { organizationAccessAuthorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId) }
  }

  @Test
  fun testUpdateOrganization() {
    every { organizationsHandler.updateOrganization(any()) } returns organizationRead
    assertNotNull(organizationApiController.updateOrganization(OrganizationUpdateRequestBody(organizationId = organizationId)))
  }

  @Test
  fun testSetOrganizationAgenticStatus() {
    val requestBody =
      OrganizationAgenticStatusUpdateRequestBody(organizationId = organizationId, isAgentic = true)
    every { organizationsHandler.setOrganizationAgenticStatus(requestBody) } returns organizationRead.copy(isAgentic = true)
    assertEquals(true, organizationApiController.setOrganizationAgenticStatus(requestBody).isAgentic)
  }

  @Test
  fun testSetOrganizationAgenticStatusFalse() {
    val requestBody =
      OrganizationAgenticStatusUpdateRequestBody(organizationId = organizationId, isAgentic = false)
    every { organizationsHandler.setOrganizationAgenticStatus(requestBody) } returns organizationRead.copy(isAgentic = false)
    assertEquals(false, organizationApiController.setOrganizationAgenticStatus(requestBody).isAgentic)
  }

  @Test
  fun testCreateOrganization() {
    every { organizationsHandler.createOrganization(any()) } returns organizationRead
    assertNotNull(
      organizationApiController.createOrganization(OrganizationCreateRequestBody(userId = UUID.randomUUID(), organizationName = "Test Organization")),
    )
  }

  @Test
  fun `getOrganizationDataWorkerUsage is not implemented in OSS`() {
    assertThrows<ApiNotImplementedInOssProblem> {
      organizationApiController.getOrganizationDataWorkerUsage(
        OrganizationDataWorkerUsageRequestBody(organizationId = organizationId, startDate = LocalDate.now(), endDate = LocalDate.now()),
      )
    }
  }

  @Test
  fun `getOrgInfo succeeds when validation passes`() {
    // Mock the currentUserService to succeed (no exception thrown)
    every { organizationAccessAuthorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId) } returns Unit

    val requestBody = OrganizationIdRequestBody(organizationId = organizationId)

    val result = organizationApiController.getOrgInfo(requestBody)

    assertNotNull(result)
    assertEquals(organizationId, result.organizationId)
    verify(exactly = 1) { organizationsHandler.getOrganizationInfo(organizationId) }
    verify(exactly = 1) { organizationAccessAuthorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId) }
  }

  @Test
  fun `getOrgInfo fails when validation fails`() {
    val forbiddenException = ForbiddenProblem()
    every { organizationAccessAuthorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId) } throws forbiddenException

    val requestBody = OrganizationIdRequestBody(organizationId = organizationId)

    val exception =
      assertThrows<ForbiddenProblem> {
        organizationApiController.getOrgInfo(requestBody)
      }

    assertEquals(forbiddenException, exception)
    verify(exactly = 0) { organizationsHandler.getOrganizationInfo(organizationId) } // Should not call handler
    verify(exactly = 1) { organizationAccessAuthorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId) }
  }
}
