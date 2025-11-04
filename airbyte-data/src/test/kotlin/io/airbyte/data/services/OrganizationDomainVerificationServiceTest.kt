/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.data.repositories.OrganizationDomainVerificationRepository
import io.airbyte.data.services.impls.data.mappers.EntityDomainVerificationMethod
import io.airbyte.data.services.impls.data.mappers.EntityDomainVerificationStatus
import io.airbyte.data.services.impls.data.mappers.EntityOrganizationDomainVerification
import io.airbyte.domain.models.DomainVerificationMethod
import io.airbyte.domain.models.DomainVerificationStatus
import io.airbyte.domain.models.OrganizationDomainVerification
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

class OrganizationDomainVerificationServiceTest {
  private val organizationDomainVerificationRepository: OrganizationDomainVerificationRepository = mockk()
  private lateinit var organizationDomainVerificationService: OrganizationDomainVerificationService

  private val testOrgId = UUID.randomUUID()
  private val testDomain = "example.com"
  private val testToken = "test-token-123"
  private val testId = UUID.randomUUID()

  @BeforeEach
  fun setup() {
    organizationDomainVerificationService = OrganizationDomainVerificationService(organizationDomainVerificationRepository)
  }

  @Test
  fun `createDomainVerification - success`() {
    val domainModel = createValidDomainModel()
    val savedEntity =
      createEntityFromDomain(domainModel).copy(
        id = testId,
        createdAt = OffsetDateTime.now(),
      )

    every { organizationDomainVerificationRepository.findByOrganizationIdAndDomain(testOrgId, testDomain) } returns null
    every { organizationDomainVerificationRepository.save(any()) } returns savedEntity

    val result =
      organizationDomainVerificationService.createDomainVerification(
        organizationId = testOrgId,
        domain = testDomain,
        createdBy = UUID.randomUUID(),
      )

    assert(result.id == testId)
    assert(result.organizationId == testOrgId)
    assert(result.domain == testDomain)
    assert(result.status == DomainVerificationStatus.PENDING)
    verify { organizationDomainVerificationRepository.findByOrganizationIdAndDomain(testOrgId, testDomain) }
    verify { organizationDomainVerificationRepository.save(any()) }
  }

  @Test
  fun `createDomainVerification - rejects duplicate domain`() {
    val domainModel = createValidDomainModel()
    val existingEntity =
      createEntityFromDomain(domainModel).copy(
        id = testId,
        status = EntityDomainVerificationStatus.verified,
      )

    every { organizationDomainVerificationRepository.findByOrganizationIdAndDomain(testOrgId, testDomain) } returns existingEntity

    val exception =
      assertThrows<IllegalArgumentException> {
        organizationDomainVerificationService.createDomainVerification(
          organizationId = testOrgId,
          domain = testDomain,
          createdBy = UUID.randomUUID(),
        )
      }
    assert(exception.message == "Domain 'example.com' is already verified for this organization.")
    verify { organizationDomainVerificationRepository.findByOrganizationIdAndDomain(testOrgId, testDomain) }
  }

  @Test
  fun `createDomainVerification - rejects expired domain`() {
    val domainModel = createValidDomainModel()
    val existingEntity =
      createEntityFromDomain(domainModel).copy(
        id = testId,
        status = EntityDomainVerificationStatus.expired,
      )

    every { organizationDomainVerificationRepository.findByOrganizationIdAndDomain(testOrgId, testDomain) } returns existingEntity

    val exception =
      assertThrows<IllegalArgumentException> {
        organizationDomainVerificationService.createDomainVerification(
          organizationId = testOrgId,
          domain = testDomain,
          createdBy = UUID.randomUUID(),
        )
      }

    assert(exception.message?.contains("expired") == true)
    verify { organizationDomainVerificationRepository.findByOrganizationIdAndDomain(testOrgId, testDomain) }
  }

  @Test
  fun `findByOrganizationId - returns list`() {
    val entity1 = createEntityFromDomain(createValidDomainModel()).copy(id = UUID.randomUUID())
    val entity2 = createEntityFromDomain(createValidDomainModel()).copy(id = UUID.randomUUID())

    every { organizationDomainVerificationRepository.findByOrganizationId(testOrgId) } returns listOf(entity1, entity2)

    val result = organizationDomainVerificationService.findByOrganizationId(testOrgId)

    assert(result.size == 2)
    verify { organizationDomainVerificationRepository.findByOrganizationId(testOrgId) }
  }

  @Test
  fun `getDomainVerification - returns verification when found`() {
    val entity = createEntityFromDomain(createValidDomainModel()).copy(id = testId)
    every { organizationDomainVerificationRepository.findById(testId) } returns Optional.of(entity)

    val result = organizationDomainVerificationService.getDomainVerification(testId)

    assert(result.id == testId)
    verify { organizationDomainVerificationRepository.findById(testId) }
  }

  @Test
  fun `getDomainVerification - throws when not found`() {
    every { organizationDomainVerificationRepository.findById(testId) } returns Optional.empty()

    val exception =
      assertThrows<IllegalArgumentException> {
        organizationDomainVerificationService.getDomainVerification(testId)
      }
    assert(exception.message == "Domain verification not found with id: $testId")
  }

  @Test
  fun `findByStatus - returns list by status`() {
    val entity =
      createEntityFromDomain(createValidDomainModel()).copy(
        id = UUID.randomUUID(),
        status = EntityDomainVerificationStatus.pending,
      )

    every { organizationDomainVerificationRepository.findByStatus(EntityDomainVerificationStatus.pending) } returns listOf(entity)

    val result = organizationDomainVerificationService.findByStatus(DomainVerificationStatus.PENDING)

    assert(result.size == 1)
    assert(result[0].status == DomainVerificationStatus.PENDING)
    verify(exactly = 1) { organizationDomainVerificationRepository.findByStatus(EntityDomainVerificationStatus.pending) }
  }

  @Test
  fun `updateVerificationStatus - updates status successfully`() {
    val entity =
      createEntityFromDomain(createValidDomainModel()).copy(
        id = testId,
        attempts = 5,
        status = EntityDomainVerificationStatus.pending,
      )
    val updatedEntity =
      entity.copy(
        status = EntityDomainVerificationStatus.verified,
        attempts = 6,
        verifiedAt = OffsetDateTime.now(),
      )

    every { organizationDomainVerificationRepository.findById(testId) } returns Optional.of(entity)
    every { organizationDomainVerificationRepository.save(any()) } returns updatedEntity

    val result = organizationDomainVerificationService.updateVerificationStatus(testId, DomainVerificationStatus.VERIFIED)

    assert(result.status == DomainVerificationStatus.VERIFIED)
    assert(result.attempts == 6)
    verify { organizationDomainVerificationRepository.save(any()) }
  }

  private fun createValidDomainModel(): OrganizationDomainVerification =
    OrganizationDomainVerification(
      id = null,
      organizationId = testOrgId,
      domain = testDomain,
      verificationMethod = DomainVerificationMethod.DNS_TXT,
      status = DomainVerificationStatus.PENDING,
      verificationToken = testToken,
      dnsRecordName = "_airbyte-verification.$testDomain",
      dnsRecordPrefix = "_airbyte-verification",
      attempts = 0,
      lastCheckedAt = null,
      expiresAt = OffsetDateTime.now().plusDays(14),
      createdBy = UUID.randomUUID(),
      verifiedAt = null,
      createdAt = null,
      updatedAt = null,
    )

  private fun createEntityFromDomain(domain: OrganizationDomainVerification): EntityOrganizationDomainVerification =
    EntityOrganizationDomainVerification(
      id = domain.id,
      organizationId = domain.organizationId,
      domain = domain.domain,
      verificationMethod = EntityDomainVerificationMethod.dns_txt,
      status = EntityDomainVerificationStatus.pending,
      verificationToken = domain.verificationToken,
      dnsRecordName = domain.dnsRecordName,
      dnsRecordPrefix = domain.dnsRecordPrefix,
      attempts = domain.attempts,
      lastCheckedAt = domain.lastCheckedAt,
      expiresAt = domain.expiresAt,
      createdBy = domain.createdBy,
      verifiedAt = domain.verifiedAt,
      createdAt = domain.createdAt,
      updatedAt = domain.updatedAt,
    )
}
