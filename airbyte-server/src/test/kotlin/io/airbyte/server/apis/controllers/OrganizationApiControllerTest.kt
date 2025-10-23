/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.OrganizationCreateRequestBody
import io.airbyte.api.model.generated.OrganizationIdRequestBody
import io.airbyte.api.model.generated.OrganizationInfoRead
import io.airbyte.api.model.generated.OrganizationRead
import io.airbyte.api.model.generated.OrganizationUpdateRequestBody
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.server.handlers.OrganizationsHandler
import io.airbyte.domain.services.dataworker.DataWorkerUsageService
import io.airbyte.server.helpers.OrganizationAccessAuthorizationHelper
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class OrganizationApiControllerTest {
  private lateinit var organizationsHandler: OrganizationsHandler
  private lateinit var organizationAccessAuthorizationHelper: OrganizationAccessAuthorizationHelper
  private lateinit var dataWorkerUsageService: DataWorkerUsageService

  private lateinit var organizationApiController: OrganizationApiController

  private val organizationId = UUID.randomUUID()
  private val organizationInfoRead =
    OrganizationInfoRead()
      .organizationId(organizationId)
      .organizationName("Test Organization")
      .sso(false)

  @BeforeEach
  fun setup() {
    organizationsHandler = mockk()
    organizationAccessAuthorizationHelper = mockk(relaxed = true)
    dataWorkerUsageService = mockk()
    organizationApiController = OrganizationApiController(organizationsHandler, organizationAccessAuthorizationHelper, dataWorkerUsageService)

    // Default behavior: organizationsHandler returns organization info
    every { organizationsHandler.getOrganizationInfo(organizationId) } returns organizationInfoRead
  }

  @Test
  fun testGetOrganization() {
    every { organizationsHandler.getOrganization(any()) } returns OrganizationRead()
    val body = OrganizationIdRequestBody().organizationId(UUID.randomUUID())
    assertNotNull(organizationApiController.getOrganization(body))
  }

  @Test
  fun testUpdateOrganization() {
    every { organizationsHandler.updateOrganization(any()) } returns OrganizationRead()
    assertNotNull(organizationApiController.updateOrganization(OrganizationUpdateRequestBody()))
  }

  @Test
  fun testCreateOrganization() {
    every { organizationsHandler.createOrganization(any()) } returns OrganizationRead()
    assertNotNull(organizationApiController.createOrganization(OrganizationCreateRequestBody()))
  }

  @Test
  fun `getOrgInfo succeeds when validation passes`() {
    // Mock the currentUserService to succeed (no exception thrown)
    every { organizationAccessAuthorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId) } returns Unit

    val requestBody = OrganizationIdRequestBody().organizationId(organizationId)

    val result = organizationApiController.getOrgInfo(requestBody)

    assertNotNull(result)
    assertEquals(organizationId, result!!.organizationId)
    verify(exactly = 1) { organizationsHandler.getOrganizationInfo(organizationId) }
    verify(exactly = 1) { organizationAccessAuthorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId) }
  }

  @Test
  fun `getOrgInfo fails when validation fails`() {
    val forbiddenException = ForbiddenProblem()
    every { organizationAccessAuthorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId) } throws forbiddenException

    val requestBody = OrganizationIdRequestBody().organizationId(organizationId)

    val exception =
      assertThrows<ForbiddenProblem> {
        organizationApiController.getOrgInfo(requestBody)
      }

    assertEquals(forbiddenException, exception)
    verify(exactly = 0) { organizationsHandler.getOrganizationInfo(organizationId) } // Should not call handler
    verify(exactly = 1) { organizationAccessAuthorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId) }
  }
}
