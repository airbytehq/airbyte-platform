/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.SsoConfig
import io.airbyte.config.SsoConfigStatus
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
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

class OrganizationDomainVerificationServiceTest {
  private val organizationDomainVerificationRepository: OrganizationDomainVerificationRepository = mockk()
  private val dnsVerificationService: DnsVerificationService = mockk()
  private val ssoConfigService: SsoConfigService = mockk()
  private val organizationEmailDomainService: OrganizationEmailDomainService = mockk()
  private lateinit var organizationDomainVerificationService: OrganizationDomainVerificationService

  private val testOrgId = UUID.randomUUID()
  private val testDomain = "example.com"
  private val testToken = "test-token-123"
  private val testId = UUID.randomUUID()

  @BeforeEach
  fun setup() {
    organizationDomainVerificationService =
      OrganizationDomainVerificationService(
        organizationDomainVerificationRepository,
        dnsVerificationService,
        ssoConfigService,
        organizationEmailDomainService,
      )
  }

  @Test
  fun `createDomainVerification - success`() {
    val domainModel = createValidDomainModel()
    val savedEntity =
      createEntityFromDomain(domainModel).copy(
        id = testId,
        createdAt = OffsetDateTime.now(),
      )

    every { organizationDomainVerificationRepository.findByOrganizationIdAndDomain(testOrgId, testDomain, false) } returns emptyList()
    every { organizationDomainVerificationRepository.save(any()) } returns savedEntity

    val result =
      organizationDomainVerificationService.createDomainVerification(
        organizationId = testOrgId,
        domain = testDomain,
        createdBy = UUID.randomUUID(),
      )

    assertEquals(testId, result.id)
    assertEquals(testOrgId, result.organizationId)
    assertEquals(testDomain, result.domain)
    assertEquals(DomainVerificationStatus.PENDING, result.status)
    verify { organizationDomainVerificationRepository.findByOrganizationIdAndDomain(testOrgId, testDomain, false) }
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

    every { organizationDomainVerificationRepository.findByOrganizationIdAndDomain(testOrgId, testDomain, false) } returns listOf(existingEntity)

    val exception =
      assertThrows<IllegalArgumentException> {
        organizationDomainVerificationService.createDomainVerification(
          organizationId = testOrgId,
          domain = testDomain,
          createdBy = UUID.randomUUID(),
        )
      }
    assertEquals("Domain 'example.com' is already verified for this organization.", exception.message)
    verify { organizationDomainVerificationRepository.findByOrganizationIdAndDomain(testOrgId, testDomain, false) }
  }

  @Test
  fun `createDomainVerification - rejects expired domain`() {
    val domainModel = createValidDomainModel()
    val existingEntity =
      createEntityFromDomain(domainModel).copy(
        id = testId,
        status = EntityDomainVerificationStatus.expired,
      )

    every { organizationDomainVerificationRepository.findByOrganizationIdAndDomain(testOrgId, testDomain, false) } returns listOf(existingEntity)

    val exception =
      assertThrows<IllegalArgumentException> {
        organizationDomainVerificationService.createDomainVerification(
          organizationId = testOrgId,
          domain = testDomain,
          createdBy = UUID.randomUUID(),
        )
      }

    assertEquals(true, exception.message?.contains("expired"))
    verify { organizationDomainVerificationRepository.findByOrganizationIdAndDomain(testOrgId, testDomain, false) }
  }

  @Test
  fun `findByOrganizationId - returns list`() {
    val entity1 = createEntityFromDomain(createValidDomainModel()).copy(id = UUID.randomUUID())
    val entity2 = createEntityFromDomain(createValidDomainModel()).copy(id = UUID.randomUUID())

    every { organizationDomainVerificationRepository.findByOrganizationId(testOrgId, false) } returns listOf(entity1, entity2)

    val result = organizationDomainVerificationService.findByOrganizationId(testOrgId)

    assertEquals(2, result.size)
    verify { organizationDomainVerificationRepository.findByOrganizationId(testOrgId, false) }
  }

  @Test
  fun `getDomainVerification - returns verification when found`() {
    val entity = createEntityFromDomain(createValidDomainModel()).copy(id = testId)
    every { organizationDomainVerificationRepository.findById(testId) } returns Optional.of(entity)

    val result = organizationDomainVerificationService.getDomainVerification(testId)

    assertEquals(testId, result.id)
    verify { organizationDomainVerificationRepository.findById(testId) }
  }

  @Test
  fun `getDomainVerification - throws when not found`() {
    every { organizationDomainVerificationRepository.findById(testId) } returns Optional.empty()

    val exception =
      assertThrows<IllegalArgumentException> {
        organizationDomainVerificationService.getDomainVerification(testId)
      }
    assertEquals("Domain verification not found with id: $testId", exception.message)
  }

  @Test
  fun `findByStatus - returns list by status`() {
    val entity =
      createEntityFromDomain(createValidDomainModel()).copy(
        id = UUID.randomUUID(),
        status = EntityDomainVerificationStatus.pending,
      )

    every { organizationDomainVerificationRepository.findByStatus(EntityDomainVerificationStatus.pending, false) } returns listOf(entity)

    val result = organizationDomainVerificationService.findByStatus(DomainVerificationStatus.PENDING)

    assertEquals(1, result.size)
    assertEquals(DomainVerificationStatus.PENDING, result[0].status)
    verify(exactly = 1) { organizationDomainVerificationRepository.findByStatus(EntityDomainVerificationStatus.pending, false) }
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
    every { organizationDomainVerificationRepository.update(any()) } returns updatedEntity

    val result = organizationDomainVerificationService.updateVerificationStatus(testId, DomainVerificationStatus.VERIFIED)

    assertEquals(DomainVerificationStatus.VERIFIED, result.status)
    assertEquals(6, result.attempts)
    verify { organizationDomainVerificationRepository.update(any()) }
  }

  @Nested
  inner class CheckAndUpdateVerification {
    @Test
    fun `should update to VERIFIED when DNS matches`() {
      val entity = createEntityFromDomain(createValidDomainModel()).copy(id = testId, attempts = 3)
      val updatedEntity = entity.copy(status = EntityDomainVerificationStatus.verified, attempts = 4)

      every { organizationDomainVerificationRepository.findById(testId) } returns Optional.of(entity)
      every { dnsVerificationService.checkDomainVerification(any(), any()) } returns DnsVerificationResult.Verified
      every { organizationDomainVerificationRepository.update(any()) } returns updatedEntity
      every { ssoConfigService.getSsoConfig(testOrgId) } returns null

      val result = organizationDomainVerificationService.checkAndUpdateVerification(testId)

      assertEquals(DomainVerificationStatus.VERIFIED, result.status)
      verify { dnsVerificationService.checkDomainVerification("_airbyte-verification.$testDomain", "airbyte-domain-verification=$testToken") }
      verify { organizationDomainVerificationRepository.update(any()) }
    }

    @Test
    fun `should update to FAILED when DNS misconfigured`() {
      val entity = createEntityFromDomain(createValidDomainModel()).copy(id = testId, attempts = 3)
      val updatedEntity = entity.copy(status = EntityDomainVerificationStatus.failed, attempts = 4)

      every { organizationDomainVerificationRepository.findById(testId) } returns Optional.of(entity)
      every { dnsVerificationService.checkDomainVerification(any(), any()) } returns
        DnsVerificationResult.Misconfigured(listOf("wrong-value"))
      every { organizationDomainVerificationRepository.update(any()) } returns updatedEntity

      val result = organizationDomainVerificationService.checkAndUpdateVerification(testId)

      assertEquals(DomainVerificationStatus.FAILED, result.status)
      verify { organizationDomainVerificationRepository.update(any()) }
    }

    @Test
    fun `should stay PENDING when DNS not found and not expired`() {
      val entity =
        createEntityFromDomain(createValidDomainModel()).copy(
          id = testId,
          attempts = 3,
          expiresAt = OffsetDateTime.now().plusDays(7),
        )
      val updatedEntity = entity.copy(attempts = 4)

      every { organizationDomainVerificationRepository.findById(testId) } returns Optional.of(entity)
      every { dnsVerificationService.checkDomainVerification(any(), any()) } returns DnsVerificationResult.NotFound
      every { organizationDomainVerificationRepository.update(any()) } returns updatedEntity

      val result = organizationDomainVerificationService.checkAndUpdateVerification(testId)

      assertEquals(DomainVerificationStatus.PENDING, result.status)
      assertEquals(4, result.attempts)
    }

    @Test
    fun `should update to EXPIRED when DNS not found and past expiration`() {
      val entity =
        createEntityFromDomain(createValidDomainModel()).copy(
          id = testId,
          attempts = 3,
          expiresAt = OffsetDateTime.now().minusDays(1),
        )
      val updatedEntity = entity.copy(status = EntityDomainVerificationStatus.expired, attempts = 4)

      every { organizationDomainVerificationRepository.findById(testId) } returns Optional.of(entity)
      every { dnsVerificationService.checkDomainVerification(any(), any()) } returns DnsVerificationResult.NotFound
      every { organizationDomainVerificationRepository.update(any()) } returns updatedEntity

      val result = organizationDomainVerificationService.checkAndUpdateVerification(testId)

      assertEquals(DomainVerificationStatus.EXPIRED, result.status)
    }

    @Test
    fun `should create email domain enforcement when verified with active SSO`() {
      val entity = createEntityFromDomain(createValidDomainModel()).copy(id = testId)
      val updatedEntity = entity.copy(status = EntityDomainVerificationStatus.verified)
      val ssoConfig = mockk<SsoConfig>()

      every { organizationDomainVerificationRepository.findById(testId) } returns Optional.of(entity)
      every { dnsVerificationService.checkDomainVerification(any(), any()) } returns DnsVerificationResult.Verified
      every { organizationDomainVerificationRepository.update(any()) } returns updatedEntity
      every { ssoConfigService.getSsoConfig(testOrgId) } returns ssoConfig
      every { ssoConfig.status } returns SsoConfigStatus.ACTIVE
      every { organizationEmailDomainService.existsByOrganizationIdAndDomain(testOrgId, testDomain) } returns false
      every { organizationEmailDomainService.createEmailDomain(any()) } returns Unit

      val result = organizationDomainVerificationService.checkAndUpdateVerification(testId)

      assertEquals(DomainVerificationStatus.VERIFIED, result.status)
      verify { organizationEmailDomainService.createEmailDomain(any()) }
    }

    @Test
    fun `should not create duplicate email domain enforcement`() {
      val entity = createEntityFromDomain(createValidDomainModel()).copy(id = testId)
      val updatedEntity = entity.copy(status = EntityDomainVerificationStatus.verified)
      val ssoConfig = mockk<SsoConfig>()

      every { organizationDomainVerificationRepository.findById(testId) } returns Optional.of(entity)
      every { dnsVerificationService.checkDomainVerification(any(), any()) } returns DnsVerificationResult.Verified
      every { organizationDomainVerificationRepository.update(any()) } returns updatedEntity
      every { ssoConfigService.getSsoConfig(testOrgId) } returns ssoConfig
      every { ssoConfig.status } returns SsoConfigStatus.ACTIVE
      every { organizationEmailDomainService.existsByOrganizationIdAndDomain(testOrgId, testDomain) } returns true

      val result = organizationDomainVerificationService.checkAndUpdateVerification(testId)

      assertEquals(DomainVerificationStatus.VERIFIED, result.status)
      verify(exactly = 0) { organizationEmailDomainService.createEmailDomain(any()) }
    }

    @Test
    fun `should throw when dnsRecordName is null`() {
      val entity =
        createEntityFromDomain(createValidDomainModel()).copy(
          id = testId,
          dnsRecordName = null,
        )

      every { organizationDomainVerificationRepository.findById(testId) } returns Optional.of(entity)

      val exception =
        assertThrows<IllegalStateException> {
          organizationDomainVerificationService.checkAndUpdateVerification(testId)
        }

      assertEquals(true, exception.message?.contains("no DNS record name"))
    }
  }

  @Test
  fun `findByOrganizationId - excludes tombstoned by default`() {
    val activeEntity = createEntityFromDomain(createValidDomainModel()).copy(id = UUID.randomUUID(), tombstone = false)

    every { organizationDomainVerificationRepository.findByOrganizationId(testOrgId, false) } returns listOf(activeEntity)

    val result = organizationDomainVerificationService.findByOrganizationId(testOrgId)

    assertEquals(1, result.size)
    assertFalse(result[0].tombstone)
    verify { organizationDomainVerificationRepository.findByOrganizationId(testOrgId, false) }
  }

  @Test
  fun `findByOrganizationId - includes tombstoned when requested`() {
    val activeEntity = createEntityFromDomain(createValidDomainModel()).copy(id = UUID.randomUUID(), tombstone = false)
    val deletedEntity = createEntityFromDomain(createValidDomainModel()).copy(id = UUID.randomUUID(), tombstone = true)

    every { organizationDomainVerificationRepository.findByOrganizationId(testOrgId, true) } returns
      listOf(activeEntity, deletedEntity)

    val result = organizationDomainVerificationService.findByOrganizationId(testOrgId, includeDeleted = true)

    assertEquals(2, result.size)
    verify { organizationDomainVerificationRepository.findByOrganizationId(testOrgId, true) }
  }

  @Nested
  inner class DeleteDomainVerification {
    @Test
    fun `successfully soft deletes and cascades to email domain`() {
      val entity = createEntityFromDomain(createValidDomainModel()).copy(id = testId, tombstone = false)
      val updatedEntity = entity.copy(tombstone = true)

      every { organizationDomainVerificationRepository.findById(testId) } returns Optional.of(entity)
      every { organizationDomainVerificationRepository.update(any()) } returns updatedEntity
      every { organizationEmailDomainService.deleteByOrganizationIdAndDomain(testOrgId, testDomain) } returns Unit

      organizationDomainVerificationService.deleteDomainVerification(testId)

      verify { organizationDomainVerificationRepository.update(match { it.tombstone == true }) }
      verify { organizationEmailDomainService.deleteByOrganizationIdAndDomain(testOrgId, testDomain) }
    }

    @Test
    fun `throws when not found`() {
      every { organizationDomainVerificationRepository.findById(testId) } returns Optional.empty()

      val exception =
        assertThrows<IllegalArgumentException> {
          organizationDomainVerificationService.deleteDomainVerification(testId)
        }
      assertEquals("Domain verification not found with id $testId", exception.message)
    }

    @Test
    fun `idempotent when already deleted`() {
      val alreadyDeletedEntity =
        createEntityFromDomain(createValidDomainModel()).copy(
          id = testId,
          tombstone = true,
        )

      every { organizationDomainVerificationRepository.findById(testId) } returns Optional.of(alreadyDeletedEntity)

      organizationDomainVerificationService.deleteDomainVerification(testId)

      verify(exactly = 0) { organizationDomainVerificationRepository.update(any()) }
      verify(exactly = 0) { organizationEmailDomainService.deleteByOrganizationIdAndDomain(any(), any()) }
    }

    @Test
    fun `throws exception and rolls back transaction if cascade fails`() {
      val entity = createEntityFromDomain(createValidDomainModel()).copy(id = testId, tombstone = false)
      val updatedEntity = entity.copy(tombstone = true)

      every { organizationDomainVerificationRepository.findById(testId) } returns Optional.of(entity)
      every { organizationDomainVerificationRepository.update(any()) } returns updatedEntity
      every { organizationEmailDomainService.deleteByOrganizationIdAndDomain(testOrgId, testDomain) } throws
        RuntimeException("Database connection failed")

      val exception =
        assertThrows<RuntimeException> {
          organizationDomainVerificationService.deleteDomainVerification(testId)
        }

      assertEquals("Database connection failed", exception.message)
      verify { organizationDomainVerificationRepository.update(match { it.tombstone == true }) }
      verify { organizationEmailDomainService.deleteByOrganizationIdAndDomain(testOrgId, testDomain) }
    }
  }

  @Nested
  inner class ResetDomainVerification {
    @Test
    fun `successfully resets FAILED verification`() {
      val entity =
        createEntityFromDomain(createValidDomainModel()).copy(
          id = testId,
          status = EntityDomainVerificationStatus.failed,
          attempts = 10,
          lastCheckedAt = OffsetDateTime.now(),
          expiresAt = OffsetDateTime.now().plusDays(2),
        )
      val resetEntity =
        entity.copy(
          status = EntityDomainVerificationStatus.pending,
          attempts = 0,
          lastCheckedAt = null,
          expiresAt = OffsetDateTime.now().plusDays(14),
        )

      every { organizationDomainVerificationRepository.findById(testId) } returns Optional.of(entity)
      every { organizationDomainVerificationRepository.update(any()) } returns resetEntity

      val result = organizationDomainVerificationService.resetDomainVerification(testId)

      assertEquals(DomainVerificationStatus.PENDING, result.status)
      assertEquals(0, result.attempts)
      assertTrue(result.expiresAt!!.isAfter(OffsetDateTime.now().plusDays(13)))
      verify {
        organizationDomainVerificationRepository.update(
          match {
            it.expiresAt!!.isAfter(OffsetDateTime.now().plusDays(13))
          },
        )
      }
    }

    @Test
    fun `successfully resets EXPIRED verification and extends expiration`() {
      val oldExpiration = OffsetDateTime.now().minusDays(5)
      val entity =
        createEntityFromDomain(createValidDomainModel()).copy(
          id = testId,
          status = EntityDomainVerificationStatus.expired,
          attempts = 50,
          expiresAt = oldExpiration,
        )
      val newExpiration = OffsetDateTime.now().plusDays(14)
      val resetEntity =
        entity.copy(
          status = EntityDomainVerificationStatus.pending,
          attempts = 0,
          lastCheckedAt = null,
          expiresAt = newExpiration,
        )

      every { organizationDomainVerificationRepository.findById(testId) } returns Optional.of(entity)
      every { organizationDomainVerificationRepository.update(any()) } returns resetEntity

      val result = organizationDomainVerificationService.resetDomainVerification(testId)

      assertEquals(DomainVerificationStatus.PENDING, result.status)
      assertEquals(0, result.attempts)
      assertTrue(result.expiresAt!!.isAfter(OffsetDateTime.now()))
      verify { organizationDomainVerificationRepository.update(any()) }
    }

    @Test
    fun `throws when verification not found`() {
      every { organizationDomainVerificationRepository.findById(testId) } returns Optional.empty()

      val exception =
        assertThrows<IllegalArgumentException> {
          organizationDomainVerificationService.resetDomainVerification(testId)
        }
      assertEquals("Domain verification not found with id $testId", exception.message)
    }

    @Test
    fun `throws when trying to reset verification in invalid state`() {
      val entity =
        createEntityFromDomain(createValidDomainModel()).copy(
          id = testId,
          status = EntityDomainVerificationStatus.pending,
        )

      every { organizationDomainVerificationRepository.findById(testId) } returns Optional.of(entity)

      val exception =
        assertThrows<IllegalArgumentException> {
          organizationDomainVerificationService.resetDomainVerification(testId)
        }
      assertEquals(true, exception.message?.contains("already pending"))
    }
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
      tombstone = false,
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
      tombstone = domain.tombstone,
    )
}
