/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.OrganizationDomainVerification
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.DomainVerificationMethod
import io.airbyte.db.instance.configs.jooq.generated.enums.DomainVerificationStatus
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.AfterEach
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
    assert(countBeforeSave == 0L)

    val saveResult = organizationDomainVerificationRepository.save(verification)
    val countAfterSave = organizationDomainVerificationRepository.count()
    assert(countAfterSave == 1L)

    val persisted = organizationDomainVerificationRepository.findById(saveResult.id!!).get()
    assert(persisted.organizationId == organizationId)
    assert(persisted.domain == "example.com")
    assert(persisted.status == DomainVerificationStatus.pending)
    assert(persisted.verificationMethod == DomainVerificationMethod.dns_txt)
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

    val org1Domains = organizationDomainVerificationRepository.findByOrganizationId(org1)
    assert(org1Domains.size == 2)
    assert(org1Domains.all { it.organizationId == org1 })

    val org2Domains = organizationDomainVerificationRepository.findByOrganizationId(org2)
    assert(org2Domains.size == 1)
    assert(org2Domains[0].domain == "other.com")
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

    val found = organizationDomainVerificationRepository.findByOrganizationIdAndDomain(org1, "example.com")
    assert(found != null)
    assert(found!!.organizationId == org1)
    assert(found.domain == "example.com")
    assert(found.status == DomainVerificationStatus.verified)

    val notFound = organizationDomainVerificationRepository.findByOrganizationIdAndDomain(org1, "test.com")
    assert(notFound == null)
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

    val pending = organizationDomainVerificationRepository.findByStatus(DomainVerificationStatus.pending)
    assert(pending.size == 2)
    assert(pending.all { it.status == DomainVerificationStatus.pending })

    val verified = organizationDomainVerificationRepository.findByStatus(DomainVerificationStatus.verified)
    assert(verified.size == 1)
    assert(verified[0].domain == "verified.com")

    val expired = organizationDomainVerificationRepository.findByStatus(DomainVerificationStatus.expired)
    assert(expired.isEmpty())
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
    assert(dnsTxt.verificationMethod == DomainVerificationMethod.dns_txt)

    val legacy =
      organizationDomainVerificationRepository.save(
        OrganizationDomainVerification(
          organizationId = orgId,
          domain = "legacy.com",
          verificationMethod = DomainVerificationMethod.legacy,
          status = DomainVerificationStatus.verified,
        ),
      )
    assert(legacy.verificationMethod == DomainVerificationMethod.legacy)
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

    assert(
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

    assert(legacy.id != null)
    assert(legacy.verificationMethod == DomainVerificationMethod.legacy)
    assert(legacy.verificationToken == null)
    assert(legacy.dnsRecordName == null)
    assert(legacy.dnsRecordPrefix == null)

    val retrieved = organizationDomainVerificationRepository.findById(legacy.id!!).get()
    assert(retrieved.verificationMethod == DomainVerificationMethod.legacy)
  }
}
