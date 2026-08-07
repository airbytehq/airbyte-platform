/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.client.WebUrlHelper
import io.airbyte.api.server.generated.models.EnableScimRequestBody
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.api.server.generated.models.ScimConfigStatus
import io.airbyte.api.server.generated.models.ScimIdpProvider
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.errors.ConflictException
import io.airbyte.commons.server.errors.IdNotFoundKnownException
import io.airbyte.commons.server.errors.OperationNotAllowedException
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.scim.ScimAccessDeniedException
import io.airbyte.domain.models.scim.ScimConfigurationConflictException
import io.airbyte.domain.models.scim.ScimConfigurationRead
import io.airbyte.domain.models.scim.ScimConfigurationStatus
import io.airbyte.domain.models.scim.ScimOrganizationNotFoundException
import io.airbyte.domain.services.scim.ScimAccessGate
import io.airbyte.domain.services.scim.ScimConfigurationService
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Status
import io.micronaut.security.annotation.Secured
import io.mockk.Runs
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

class ScimConfigApiControllerTest {
  private val service = mockk<ScimConfigurationService>()
  private val scimAccessGate = mockk<ScimAccessGate>()
  private val currentUserService = mockk<CurrentUserService>()
  private val webUrlHelper =
    mockk<WebUrlHelper> {
      every { baseUrl } returns "https://airbyte.example.com"
    }
  private val controller = ScimConfigApiController(service, scimAccessGate, currentUserService, webUrlHelper)
  private val organizationId = UUID.randomUUID()
  private val userId = UUID.randomUUID()

  @BeforeEach
  fun setUp() {
    clearMocks(service, scimAccessGate, currentUserService)
    every { scimAccessGate.isAllowed(any()) } returns false
    every { currentUserService.getCurrentUser() } returns
      mockk {
        every { this@mockk.userId } returns this@ScimConfigApiControllerTest.userId
      }
  }

  @Test
  fun `get maps not configured status and absolute SCIM base URL`() {
    every { service.getConfiguration(OrganizationId(organizationId)) } returns
      ScimConfigurationRead(status = ScimConfigurationStatus.NOT_CONFIGURED)
    every { scimAccessGate.isAllowed(OrganizationId(organizationId)) } returns false

    val result = controller.getScimConfig(OrganizationIdRequestBody(organizationId))

    assertEquals(ScimConfigStatus.NOT_CONFIGURED, result.status)
    assertEquals("https://airbyte.example.com/scim/v2", result.scimBaseUrl)
    assertNull(result.idpProvider)
    assertNull(result.createdAt)
    assertNull(result.updatedAt)
    assertNull(result.token)
    assertEquals(false, result.available)
    verify(exactly = 0) { currentUserService.getCurrentUser() }
  }

  @Test
  fun `get reports available true when the access gate allows the organization`() {
    every { service.getConfiguration(OrganizationId(organizationId)) } returns
      ScimConfigurationRead(status = ScimConfigurationStatus.NOT_CONFIGURED)
    every { scimAccessGate.isAllowed(OrganizationId(organizationId)) } returns true

    val result = controller.getScimConfig(OrganizationIdRequestBody(organizationId))

    assertEquals(ScimConfigStatus.NOT_CONFIGURED, result.status)
    assertEquals(true, result.available)
  }

  @Test
  fun `enable maps provider actor timestamps and one-time token`() {
    val createdAt = OffsetDateTime.of(2026, 7, 14, 1, 2, 3, 0, ZoneOffset.UTC)
    val updatedAt = createdAt.plusMinutes(1)
    every {
      service.enable(
        OrganizationId(organizationId),
        io.airbyte.domain.models.scim.ScimIdpProvider.MICROSOFT_ENTRA_ID,
        UserId(userId),
      )
    } returns
      ScimConfigurationRead(
        status = ScimConfigurationStatus.ENABLED,
        idpProvider = io.airbyte.domain.models.scim.ScimIdpProvider.MICROSOFT_ENTRA_ID,
        createdAt = createdAt,
        updatedAt = updatedAt,
        token = "airbyte_scim_one_time_token",
      )

    val result =
      controller.enableScim(
        EnableScimRequestBody(organizationId, ScimIdpProvider.MICROSOFT_ENTRA_ID),
      )

    assertEquals(ScimConfigStatus.ENABLED, result.status)
    assertEquals(ScimIdpProvider.MICROSOFT_ENTRA_ID, result.idpProvider)
    assertEquals(createdAt.toEpochSecond(), result.createdAt)
    assertEquals(updatedAt.toEpochSecond(), result.updatedAt)
    assertEquals("airbyte_scim_one_time_token", result.token)
    // A successful enable implies the organization passed the gate inside the service.
    assertEquals(true, result.available)
    verify(exactly = 0) { scimAccessGate.isAllowed(any()) }
  }

  @Test
  fun `rotate delegates with current actor and returns a new token`() {
    every { service.rotateToken(OrganizationId(organizationId), UserId(userId)) } returns
      ScimConfigurationRead(
        status = ScimConfigurationStatus.ENABLED,
        idpProvider = io.airbyte.domain.models.scim.ScimIdpProvider.OKTA,
        token = "airbyte_scim_rotated_token",
      )

    val result = controller.rotateScimToken(OrganizationIdRequestBody(organizationId))

    assertEquals("airbyte_scim_rotated_token", result.token)
    // A successful rotation implies the organization passed the gate inside the service.
    assertEquals(true, result.available)
    verify(exactly = 1) { service.rotateToken(OrganizationId(organizationId), UserId(userId)) }
    verify(exactly = 0) { scimAccessGate.isAllowed(any()) }
  }

  @Test
  fun `disable delegates with current actor`() {
    every { service.disable(OrganizationId(organizationId), UserId(userId)) } just Runs

    controller.disableScim(OrganizationIdRequestBody(organizationId))

    verify(exactly = 1) { service.disable(OrganizationId(organizationId), UserId(userId)) }
  }

  @Test
  fun `access denial maps to a 403 known exception`() {
    every { service.rotateToken(any(), any()) } throws ScimAccessDeniedException("not available")

    val error =
      assertThrows<OperationNotAllowedException> {
        controller.rotateScimToken(OrganizationIdRequestBody(organizationId))
      }

    assertEquals("not available", error.message)
  }

  @Test
  fun `lifecycle conflict maps to a 409 known exception`() {
    every { service.enable(any(), any(), any()) } throws ScimConfigurationConflictException("already disabled")

    val error =
      assertThrows<ConflictException> {
        controller.enableScim(EnableScimRequestBody(organizationId, ScimIdpProvider.OKTA))
      }

    assertEquals("already disabled", error.message)
  }

  @Test
  fun `missing organization maps to a not found known exception`() {
    every { service.disable(any(), any()) } throws ScimOrganizationNotFoundException(organizationId)

    val error =
      assertThrows<IdNotFoundKnownException> {
        controller.disableScim(OrganizationIdRequestBody(organizationId))
      }

    assertEquals(organizationId.toString(), error.id)
  }

  @Test
  fun `all SCIM endpoints require organization admin and use the SCIM audit provider`() {
    val endpointNames = setOf("getScimConfig", "enableScim", "rotateScimToken", "disableScim")
    val endpoints = ScimConfigApiController::class.java.declaredMethods.filter { it.name in endpointNames }

    assertEquals(endpointNames, endpoints.map { it.name }.toSet())
    endpoints.forEach { endpoint ->
      assertEquals(AuditLoggingProvider.SCIM, endpoint.getAnnotation(AuditLogging::class.java).provider)
      assertEquals(
        listOf(AuthRoleConstants.ORGANIZATION_ADMIN),
        endpoint.getAnnotation(Secured::class.java).value.toList(),
      )
    }

    val disableEndpoint = endpoints.single { it.name == "disableScim" }
    assertEquals(HttpStatus.NO_CONTENT, disableEndpoint.getAnnotation(Status::class.java).value)
  }
}
