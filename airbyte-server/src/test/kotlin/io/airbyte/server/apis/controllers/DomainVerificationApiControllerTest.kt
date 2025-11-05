/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.DomainVerificationCreateRequestBody
import io.airbyte.api.server.generated.models.DomainVerificationResponse
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.data.services.OrganizationDomainVerificationService
import io.airbyte.domain.models.DomainVerificationMethod
import io.airbyte.domain.models.DomainVerificationStatus
import io.airbyte.domain.models.OrganizationDomainVerification
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.UUID

class DomainVerificationApiControllerTest {
  private lateinit var organizationDomainVerificationService: OrganizationDomainVerificationService
  private lateinit var roleResolver: RoleResolver
  private lateinit var currentUserService: CurrentUserService
  private lateinit var domainVerificationApiController: DomainVerificationApiController

  private val testOrgId = UUID.randomUUID()
  private val testUserId = UUID.randomUUID()
  private val testDomain = "example.com"
  private val testId = UUID.randomUUID()
  private val testToken = "test-token-123"

  @BeforeEach
  fun setup() {
    organizationDomainVerificationService = mockk()
    roleResolver = mockk(relaxed = true)
    currentUserService = mockk()

    domainVerificationApiController =
      DomainVerificationApiController(
        organizationDomainVerificationService,
        currentUserService,
        roleResolver,
      )

    every { currentUserService.getCurrentUser() } returns
      mockk {
        every { userId } returns testUserId
      }
  }

  @Test
  fun `createDomainVerification - success with all fields populated`() {
    val requestBody =
      DomainVerificationCreateRequestBody(
        organizationId = testOrgId,
        domain = testDomain,
      )

    val createdDomainModel =
      createDomainModel(
        id = testId,
        domain = testDomain,
        status = DomainVerificationStatus.PENDING,
      )

    every {
      organizationDomainVerificationService.createDomainVerification(
        organizationId = testOrgId,
        domain = testDomain,
        createdBy = any(),
      )
    } returns createdDomainModel

    val result = domainVerificationApiController.createDomainVerification(requestBody)

    assertNotNull(result)
    assertEquals(testId, result.id)
    assertEquals(testOrgId, result.organizationId)
    assertEquals(testDomain, result.domain)
    assertEquals(DomainVerificationResponse.Status.PENDING, result.status)
    assertEquals(DomainVerificationResponse.VerificationMethod.DNS_TXT, result.verificationMethod)
    assertEquals("_airbyte-verification.$testDomain", result.dnsRecordName)
    assertEquals("airbyte-domain-verification=$testToken", result.dnsRecordValue)
    assertNotNull(result.expiresAt)
    assertEquals(0, result.attempts)

    verify(exactly = 1) { currentUserService.getCurrentUser() }
    verify(exactly = 1) {
      organizationDomainVerificationService.createDomainVerification(
        organizationId = testOrgId,
        domain = testDomain,
        createdBy = any(),
      )
    }
  }

  @Test
  fun `createDomainVerification - rejects duplicate domain`() {
    val requestBody =
      DomainVerificationCreateRequestBody(
        organizationId = testOrgId,
        domain = testDomain,
      )

    every {
      organizationDomainVerificationService.createDomainVerification(
        organizationId = testOrgId,
        domain = testDomain,
        createdBy = any(),
      )
    } throws
      IllegalArgumentException("Domain '$testDomain' is already verified for this organization.")

    val exception =
      assertThrows<BadRequestProblem> {
        domainVerificationApiController.createDomainVerification(requestBody)
      }

    assertEquals("Domain '$testDomain' is already verified for this organization.", exception.problem.getDetail())
    verify(exactly = 1) {
      organizationDomainVerificationService.createDomainVerification(
        organizationId = testOrgId,
        domain = testDomain,
        createdBy = any(),
      )
    }
  }

  @Test
  fun `listDomainVerifications - returns list of verifications`() {
    val requestBody = OrganizationIdRequestBody(organizationId = testOrgId)

    val domain1 =
      createDomainModel(
        id = UUID.randomUUID(),
        domain = "example1.com",
        status = DomainVerificationStatus.VERIFIED,
      )

    val domain2 =
      createDomainModel(
        id = UUID.randomUUID(),
        domain = "example2.com",
        status = DomainVerificationStatus.PENDING,
      )

    every { organizationDomainVerificationService.findByOrganizationId(testOrgId) } returns listOf(domain1, domain2)

    val result = domainVerificationApiController.listDomainVerifications(requestBody)

    assertNotNull(result)

    val verifications = result.domainVerifications
    assertNotNull(verifications)
    assertEquals(2, verifications.size)
    assertEquals("example1.com", verifications[0].domain)
    assertEquals(DomainVerificationResponse.Status.VERIFIED, verifications[0].status)
    assertEquals("example2.com", verifications[1].domain)
    assertEquals(DomainVerificationResponse.Status.PENDING, verifications[1].status)

    verify { organizationDomainVerificationService.findByOrganizationId(testOrgId) }
  }

  @Test
  fun `listDomainVerifications - returns empty list when no verifications`() {
    val requestBody = OrganizationIdRequestBody(organizationId = testOrgId)

    every { organizationDomainVerificationService.findByOrganizationId(testOrgId) } returns emptyList()

    val result = domainVerificationApiController.listDomainVerifications(requestBody)

    val verifications = result.domainVerifications
    assertNotNull(verifications)
    assertEquals(0, verifications.size)

    verify { organizationDomainVerificationService.findByOrganizationId(testOrgId) }
  }

  private fun createDomainModel(
    id: UUID = UUID.randomUUID(),
    domain: String = testDomain,
    status: DomainVerificationStatus = DomainVerificationStatus.PENDING,
  ): OrganizationDomainVerification =
    OrganizationDomainVerification(
      id = id,
      organizationId = testOrgId,
      domain = domain,
      verificationMethod = DomainVerificationMethod.DNS_TXT,
      status = status,
      verificationToken = testToken,
      dnsRecordName = "_airbyte-verification.$domain",
      dnsRecordPrefix = "_airbyte-verification",
      attempts = 0,
      lastCheckedAt = null,
      expiresAt = OffsetDateTime.now().plusDays(14),
      createdBy = testUserId,
      verifiedAt = if (status == DomainVerificationStatus.VERIFIED) OffsetDateTime.now() else null,
      createdAt = OffsetDateTime.now(),
      updatedAt = OffsetDateTime.now(),
    )
}
