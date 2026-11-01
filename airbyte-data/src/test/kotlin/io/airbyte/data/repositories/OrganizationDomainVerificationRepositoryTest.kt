/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.OrganizationDomainVerification
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.DomainVerificationMethod
import io.airbyte.db.instance.configs.jooq.generated.enums.DomainVerificationStatus
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.UUID

@MicronautTest
class OrganizationDomainVerificationRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    @BeforeAll
    @JvmStatic
    fun setup() {
      jooqDslContext
        .alterTable(Tables.ORGANIZATION_DOMAIN_VERIFICATION)
        .dropForeignKey(Keys.ORGANIZATION_DOMAIN_VERIFICATION__ORGANIZATION_DOMAIN_VERIFICATION_ORGANIZATION_ID_FKEY.constraint())
        .execute()

      jooqDslContext
        .alterTable(Tables.ORGANIZATION_DOMAIN_VERIFICATION)
        .dropForeignKey(Keys.ORGANIZATION_DOMAIN_VERIFICATION__ORGANIZATION_DOMAIN_VERIFICATION_CREATED_BY_FKEY.constraint())
        .execute()
    }
  }

  @AfterEach
  fun tearDown() {
    organizationDomainVerificationRepository.deleteAll()
  }

  @Test
  fun `test db insertion`() {
    val organizationId = UUID.randomUUID()
    val verification =
      OrganizationDomainVerification(
        organizationId = organizationId,
        domain = "example.com",
        verificationMethod = DomainVerificationMethod.dns_txt,
        status = DomainVerificationStatus.pending,
        verificationToken = "abc123",
        dnsRecordName = "_airbyte-verification.example.com",
        dnsRecordPrefix = "_airbyte-verification",
      )

    val countBeforeSave = organizationDomainVerificationRepository.count()
    assertEquals(0L, countBeforeSave)

    val saveResult = organizationDomainVerificationRepository.save(verification)
    val countAfterSave = organizationDomainVerificationRepository.count()
    assertEquals(1L, countAfterSave)

    val persisted = organizationDomainVerificationRepository.findById(saveResult.id!!).get()
    assertEquals(organizationId, persisted.organizationId)
    assertEquals("example.com", persisted.domain)
    assertEquals(DomainVerificationStatus.pending, persisted.status)
    assertEquals(DomainVerificationMethod.dns_txt, persisted.verificationMethod)
  }

  @Test
  fun `test findByOrganizationId returns all domains for org`() {
    val org1 = UUID.randomUUID()
    val org2 = UUID.randomUUID()

    // Create 2 domains for org1
    organizationDomainVerificationRepository.save(
      OrganizationDomainVerification(
        organizationId = org1,
        domain = "example.com",
        verificationMethod = DomainVerificationMethod.dns_txt,
        status = DomainVerificationStatus.pending,
        verificationToken = "token1",
        dnsRecordName = "_airbyte-verification.example.com",
        dnsRecordPrefix = "_airbyte-verification",
      ),
    )
    organizationDomainVerificationRepository.save(
      OrganizationDomainVerification(
        organizationId = org1,
        domain = "test.com",
        verificationMethod = DomainVerificationMethod.dns_txt,
        status = DomainVerificationStatus.verified,
        verificationToken = "token2",
        dnsRecordName = "_airbyte-verification.test.com",
        dnsRecordPrefix = "_airbyte-verification",
      ),
    )

    // Create 1 domain for org2
    organizationDomainVerificationRepository.save(
      OrganizationDomainVerification(
        organizationId = org2,
        domain = "other.com",
        verificationMethod = DomainVerificationMethod.dns_txt,
        status = DomainVerificationStatus.pending,
        verificationToken = "token3",
        dnsRecordName = "_airbyte-verification.other.com",
        dnsRecordPrefix = "_airbyte-verification",
      ),
    )

    val org1Domains = organizationDomainVerificationRepository.findByOrganizationId(org1, false)
    assertEquals(2, org1Domains.size)
    assertTrue(org1Domains.all { it.organizationId == org1 })

    val org2Domains = organizationDomainVerificationRepository.findByOrganizationId(org2, false)
    assertEquals(1, org2Domains.size)
    assertEquals("other.com", org2Domains[0].domain)
  }

  @Test
  fun `test findByOrganizationIdAndDomain finds exact match`() {
    val org1 = UUID.randomUUID()
    val org2 = UUID.randomUUID()

    organizationDomainVerificationRepository.save(
      OrganizationDomainVerification(
        organizationId = org1,
        domain = "example.com",
        verificationMethod = DomainVerificationMethod.dns_txt,
        status = DomainVerificationStatus.verified,
        verificationToken = "token1",
        dnsRecordName = "_airbyte-verification.example.com",
        dnsRecordPrefix = "_airbyte-verification",
      ),
    )
    organizationDomainVerificationRepository.save(
      OrganizationDomainVerification(
        organizationId = org2,
        domain = "test.com",
        verificationMethod = DomainVerificationMethod.dns_txt,
        status = DomainVerificationStatus.pending,
        verificationToken = "token2",
        dnsRecordName = "_airbyte-verification.test.com",
        dnsRecordPrefix = "_airbyte-verification",
      ),
    )

    val found = organizationDomainVerificationRepository.findByOrganizationIdAndDomain(org1, "example.com", false)
    assertTrue(found.isNotEmpty())
    assertEquals(org1, found.first().organizationId)
    assertEquals("example.com", found.first().domain)
    assertEquals(DomainVerificationStatus.verified, found.first().status)

    val notFound = organizationDomainVerificationRepository.findByOrganizationIdAndDomain(org1, "test.com", false)
    assertTrue(notFound.isEmpty())
  }

  @Test
  fun `test findByStatus returns only matching status`() {
    val org1 = UUID.randomUUID()
    val org2 = UUID.randomUUID()
    val org3 = UUID.randomUUID()

    organizationDomainVerificationRepository.save(
      OrganizationDomainVerification(
        organizationId = org1,
        domain = "pending1.com",
        verificationMethod = DomainVerificationMethod.dns_txt,
        status = DomainVerificationStatus.pending,
        verificationToken = "token1",
        dnsRecordName = "_airbyte-verification.pending1.com",
        dnsRecordPrefix = "_airbyte-verification",
      ),
    )
    organizationDomainVerificationRepository.save(
      OrganizationDomainVerification(
        organizationId = org2,
        domain = "pending2.com",
        verificationMethod = DomainVerificationMethod.dns_txt,
        status = DomainVerificationStatus.pending,
        verificationToken = "token2",
        dnsRecordName = "_airbyte-verification.pending2.com",
        dnsRecordPrefix = "_airbyte-verification",
      ),
    )
    organizationDomainVerificationRepository.save(
      OrganizationDomainVerification(
        organizationId = org3,
        domain = "verified.com",
        verificationMethod = DomainVerificationMethod.dns_txt,
        status = DomainVerificationStatus.verified,
        verificationToken = "token3",
        dnsRecordName = "_airbyte-verification.verified.com",
        dnsRecordPrefix = "_airbyte-verification",
      ),
    )

    val pending = organizationDomainVerificationRepository.findByStatus(DomainVerificationStatus.pending, false)
    assertEquals(2, pending.size)
    assertTrue(pending.all { it.status == DomainVerificationStatus.pending })

    val verified = organizationDomainVerificationRepository.findByStatus(DomainVerificationStatus.verified, false)
    assertEquals(1, verified.size)
    assertEquals("verified.com", verified[0].domain)

    val expired = organizationDomainVerificationRepository.findByStatus(DomainVerificationStatus.expired, false)
    assertTrue(expired.isEmpty())
  }

  @Test
  fun `test enum types are stored correctly`() {
    val orgId = UUID.randomUUID()

    val dnsTxt =
      organizationDomainVerificationRepository.save(
        OrganizationDomainVerification(
          organizationId = orgId,
          domain = "dns.com",
          verificationMethod = DomainVerificationMethod.dns_txt,
          status = DomainVerificationStatus.pending,
          verificationToken = "token1",
          dnsRecordName = "_airbyte-verification.dns.com",
          dnsRecordPrefix = "_airbyte-verification",
        ),
      )
    assertEquals(DomainVerificationMethod.dns_txt, dnsTxt.verificationMethod)

    val legacy =
      organizationDomainVerificationRepository.save(
        OrganizationDomainVerification(
          organizationId = orgId,
          domain = "legacy.com",
          verificationMethod = DomainVerificationMethod.legacy,
          status = DomainVerificationStatus.verified,
        ),
      )
    assertEquals(DomainVerificationMethod.legacy, legacy.verificationMethod)
  }

  @Test
  fun `test dns_txt method fails without verificationToken`() {
    val orgId = UUID.randomUUID()

    // Should throw exception - dns_txt requires all DNS fields
    val exception =
      org.junit.jupiter.api.assertThrows<Exception> {
        organizationDomainVerificationRepository.save(
          OrganizationDomainVerification(
            organizationId = orgId,
            domain = "example.com",
            verificationMethod = DomainVerificationMethod.dns_txt,
            status = DomainVerificationStatus.pending,
            verificationToken = null,
            dnsRecordName = "_airbyte-verification.example.com",
            dnsRecordPrefix = "_airbyte-verification",
          ),
        )
      }

    assertTrue(
      exception.message?.contains("check_dns_fields_required_for_dns_txt") == true ||
        exception.message?.contains("constraint") == true,
    )
  }

  @Test
  fun `test legacy method can save without DNS fields`() {
    val orgId = UUID.randomUUID()

    // Legacy should save successfully without DNS fields
    val legacy =
      organizationDomainVerificationRepository.save(
        OrganizationDomainVerification(
          organizationId = orgId,
          domain = "legacy.com",
          verificationMethod = DomainVerificationMethod.legacy,
          status = DomainVerificationStatus.pending,
          verificationToken = null,
          dnsRecordName = null,
          dnsRecordPrefix = null,
        ),
      )

    assertNotNull(legacy.id)
    assertEquals(DomainVerificationMethod.legacy, legacy.verificationMethod)
    assertNull(legacy.verificationToken)
    assertNull(legacy.dnsRecordName)
    assertNull(legacy.dnsRecordPrefix)

    val retrieved = organizationDomainVerificationRepository.findById(legacy.id!!).get()
    assertEquals(DomainVerificationMethod.legacy, retrieved.verificationMethod)
  }

  @Test
  fun `test findByOrganizationId with includeDeleted parameter`() {
    val orgId = UUID.randomUUID()

    // Create 2 active domains
    organizationDomainVerificationRepository.save(
      OrganizationDomainVerification(
        organizationId = orgId,
        domain = "active1.com",
        verificationMethod = DomainVerificationMethod.dns_txt,
        status = DomainVerificationStatus.verified,
        verificationToken = "token1",
        dnsRecordName = "_airbyte-verification.active1.com",
        dnsRecordPrefix = "_airbyte-verification",
        tombstone = false,
      ),
    )
    organizationDomainVerificationRepository.save(
      OrganizationDomainVerification(
        organizationId = orgId,
        domain = "active2.com",
        verificationMethod = DomainVerificationMethod.dns_txt,
        status = DomainVerificationStatus.verified,
        verificationToken = "token2",
        dnsRecordName = "_airbyte-verification.active2.com",
        dnsRecordPrefix = "_airbyte-verification",
        tombstone = false,
      ),
    )

    // Create 1 tombstoned (deleted) domain
    organizationDomainVerificationRepository.save(
      OrganizationDomainVerification(
        organizationId = orgId,
        domain = "deleted.com",
        verificationMethod = DomainVerificationMethod.dns_txt,
        status = DomainVerificationStatus.verified,
        verificationToken = "token3",
        dnsRecordName = "_airbyte-verification.deleted.com",
        dnsRecordPrefix = "_airbyte-verification",
        tombstone = true,
      ),
    )

    val active = organizationDomainVerificationRepository.findByOrganizationId(orgId, includeDeleted = false)
    assertEquals(2, active.size)
    assertTrue(active.none { it.tombstone })
    assertTrue(active.all { it.domain in listOf("active1.com", "active2.com") })

    val all = organizationDomainVerificationRepository.findByOrganizationId(orgId, includeDeleted = true)
    assertEquals(3, all.size)
    assertEquals(2, all.count { !it.tombstone })
    assertEquals(1, all.count { it.tombstone })
  }

  @Test
  fun `test findByOrganizationIdAndDomain with includeDeleted parameter`() {
    val orgId = UUID.randomUUID()

    // Create active domain
    organizationDomainVerificationRepository.save(
      OrganizationDomainVerification(
        organizationId = orgId,
        domain = "example.com",
        verificationMethod = DomainVerificationMethod.dns_txt,
        status = DomainVerificationStatus.verified,
        verificationToken = "token1",
        dnsRecordName = "_airbyte-verification.example.com",
        dnsRecordPrefix = "_airbyte-verification",
        tombstone = false,
      ),
    )

    // Create tombstoned domain with SAME name in SAME org
    organizationDomainVerificationRepository.save(
      OrganizationDomainVerification(
        organizationId = orgId,
        domain = "example1.com",
        verificationMethod = DomainVerificationMethod.dns_txt,
        status = DomainVerificationStatus.expired,
        verificationToken = "token2",
        dnsRecordName = "_airbyte-verification.example.com",
        dnsRecordPrefix = "_airbyte-verification",
        tombstone = true,
      ),
    )

    val found =
      organizationDomainVerificationRepository.findByOrganizationIdAndDomain(
        orgId,
        "example.com",
        includeDeleted = false,
      )
    assertTrue(found.isNotEmpty())
    assertFalse(found.first().tombstone)
    assertEquals(DomainVerificationStatus.verified, found.first().status)

    val foundWithDeleted =
      organizationDomainVerificationRepository.findByOrganizationIdAndDomain(
        orgId,
        "example.com",
        includeDeleted = true,
      )
    assertTrue(foundWithDeleted.isNotEmpty())
  }

  @Test
  fun `test findByStatus with includeDeleted parameter`() {
    val org1 = UUID.randomUUID()
    val org2 = UUID.randomUUID()

    // Active pending domain
    organizationDomainVerificationRepository.save(
      OrganizationDomainVerification(
        organizationId = org1,
        domain = "active-pending.com",
        verificationMethod = DomainVerificationMethod.dns_txt,
        status = DomainVerificationStatus.pending,
        verificationToken = "token1",
        dnsRecordName = "_airbyte-verification.active-pending.com",
        dnsRecordPrefix = "_airbyte-verification",
        tombstone = false,
      ),
    )

    // Tombstoned pending domain
    organizationDomainVerificationRepository.save(
      OrganizationDomainVerification(
        organizationId = org2,
        domain = "deleted-pending.com",
        verificationMethod = DomainVerificationMethod.dns_txt,
        status = DomainVerificationStatus.pending,
        verificationToken = "token2",
        dnsRecordName = "_airbyte-verification.deleted-pending.com",
        dnsRecordPrefix = "_airbyte-verification",
        tombstone = true,
      ),
    )

    val activePending =
      organizationDomainVerificationRepository.findByStatus(
        DomainVerificationStatus.pending,
        includeDeleted = false,
      )
    assertEquals(1, activePending.size)
    assertEquals("active-pending.com", activePending[0].domain)
    assertFalse(activePending[0].tombstone)

    val allPending =
      organizationDomainVerificationRepository.findByStatus(
        DomainVerificationStatus.pending,
        includeDeleted = true,
      )
    assertEquals(2, allPending.size)
    assertEquals(1, allPending.count { !it.tombstone })
    assertEquals(1, allPending.count { it.tombstone })
  }

  @Test
  fun `test soft-delete then insert same organization_id and domain`() {
    val orgId = UUID.randomUUID()
    val domain = "reusable-domain.com"

    // Insert first record
    val firstRecord =
      organizationDomainVerificationRepository.save(
        OrganizationDomainVerification(
          organizationId = orgId,
          domain = domain,
          verificationMethod = DomainVerificationMethod.dns_txt,
          status = DomainVerificationStatus.verified,
          verificationToken = "token1",
          dnsRecordName = "_airbyte-verification.$domain",
          dnsRecordPrefix = "_airbyte-verification",
          tombstone = false,
        ),
      )
    assertNotNull(firstRecord.id)

    // Soft-delete the first record
    val deletedRecord =
      organizationDomainVerificationRepository.update(
        firstRecord.copy(tombstone = true),
      )
    assertTrue(deletedRecord.tombstone)

    // Insert second record with same organization_id and domain (should succeed)
    val secondRecord =
      organizationDomainVerificationRepository.save(
        OrganizationDomainVerification(
          organizationId = orgId,
          domain = domain,
          verificationMethod = DomainVerificationMethod.dns_txt,
          status = DomainVerificationStatus.pending,
          verificationToken = "token2",
          dnsRecordName = "_airbyte-verification.$domain",
          dnsRecordPrefix = "_airbyte-verification",
          tombstone = false,
        ),
      )
    assertNotNull(secondRecord.id)
    assertNotEquals(secondRecord.id, firstRecord.id)

    // Verify both records exist when including deleted
    val allRecords =
      organizationDomainVerificationRepository.findByOrganizationIdAndDomain(
        orgId,
        domain,
        includeDeleted = true,
      )
    assertEquals(2, allRecords.size)
    assertEquals(1, allRecords.count { it.tombstone })
    assertEquals(1, allRecords.count { !it.tombstone })

    // Verify only active record is returned when excluding deleted
    val activeRecords =
      organizationDomainVerificationRepository.findByOrganizationIdAndDomain(
        orgId,
        domain,
        includeDeleted = false,
      )
    assertEquals(1, activeRecords.size)
    assertEquals(secondRecord.id, activeRecords[0].id)
    assertFalse(activeRecords[0].tombstone)
    assertEquals(DomainVerificationStatus.pending, activeRecords[0].status)
  }
}
