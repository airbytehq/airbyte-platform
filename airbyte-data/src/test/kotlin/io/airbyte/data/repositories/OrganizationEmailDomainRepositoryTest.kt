/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.OrganizationEmailDomain
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.UUID

@MicronautTest
internal class OrganizationEmailDomainRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    @BeforeAll
    @JvmStatic
    fun setup() {
      // so we don't have to deal with making orgs as well
      jooqDslContext
        .alterTable(
          Tables.ORGANIZATION_EMAIL_DOMAIN,
        ).dropForeignKey(Keys.ORGANIZATION_EMAIL_DOMAIN__ORGANIZATION_EMAIL_DOMAIN_ORGANIZATION_ID_FKEY.constraint())
        .execute()
    }
  }

  @AfterEach
  fun tearDown() {
    organizationEmailDomainRepository.deleteAll()
  }

  @Test
  fun `test db insertion`() {
    val organizationId = UUID.randomUUID()
    val orgEmailDomain =
      OrganizationEmailDomain(
        organizationId = organizationId,
        emailDomain = "airbyte.io",
      )

    val countBeforeSave = organizationEmailDomainRepository.count()
    assert(countBeforeSave == 0L)

    val saveResult = organizationEmailDomainRepository.save(orgEmailDomain)
    val countAfterSave = organizationEmailDomainRepository.count()
    assert(countAfterSave == 1L)

    val persistedOrgEmailDomain = organizationEmailDomainRepository.findById(saveResult.id!!).get()
    assert(persistedOrgEmailDomain.organizationId == organizationId)
    assert(persistedOrgEmailDomain.emailDomain == "airbyte.io")
  }

  @Test
  fun `test find by email domain`() {
    assert(organizationEmailDomainRepository.findByEmailDomainIgnoreCase("airbyte.io").isEmpty())

    val organizationId = UUID.randomUUID()
    val orgEmailDomain =
      OrganizationEmailDomain(
        organizationId = organizationId,
        emailDomain = "airbyte.io",
      )

    organizationEmailDomainRepository.save(orgEmailDomain)

    val otherOrgId = UUID.randomUUID()
    val otherOrgEmailDomain =
      OrganizationEmailDomain(
        organizationId = otherOrgId,
        emailDomain = "airbyte.dev",
      )
    organizationEmailDomainRepository.save(otherOrgEmailDomain)

    val orgEmailDomains = organizationEmailDomainRepository.findByEmailDomainIgnoreCase("airbyte.io")
    assert(orgEmailDomains.size == 1)
    assert(orgEmailDomains[0].organizationId == organizationId)
    assert(orgEmailDomains[0].emailDomain == "airbyte.io")
  }

  @Test
  fun `test find by email domain is case insensitive`() {
    val organizationId = UUID.randomUUID()
    val orgEmailDomain =
      OrganizationEmailDomain(
        organizationId = organizationId,
        emailDomain = "Airbyte.IO",
      )

    organizationEmailDomainRepository.save(orgEmailDomain)

    // Search with lowercase should find the domain stored with mixed case
    val resultLowercase = organizationEmailDomainRepository.findByEmailDomainIgnoreCase("airbyte.io")
    assert(resultLowercase.size == 1)
    assert(resultLowercase[0].organizationId == organizationId)
    assert(resultLowercase[0].emailDomain == "Airbyte.IO")

    // Search with uppercase should also find it
    val resultUppercase = organizationEmailDomainRepository.findByEmailDomainIgnoreCase("AIRBYTE.IO")
    assert(resultUppercase.size == 1)
    assert(resultUppercase[0].organizationId == organizationId)
    assert(resultUppercase[0].emailDomain == "Airbyte.IO")

    // Search with different mixed case should also find it
    val resultMixed = organizationEmailDomainRepository.findByEmailDomainIgnoreCase("AiRbYtE.iO")
    assert(resultMixed.size == 1)
    assert(resultMixed[0].organizationId == organizationId)
    assert(resultMixed[0].emailDomain == "Airbyte.IO")
  }

  @Test
  fun `delete by organization id removes all related rows`() {
    val organizationId = UUID.randomUUID()
    val orgEmailDomain1 =
      OrganizationEmailDomain(
        organizationId = organizationId,
        emailDomain = "airbyte.io",
      )
    organizationEmailDomainRepository.save(orgEmailDomain1)

    val orgEmailDomain2 =
      OrganizationEmailDomain(
        organizationId = organizationId,
        emailDomain = "airbyte.com",
      )
    organizationEmailDomainRepository.save(orgEmailDomain2)

    organizationEmailDomainRepository.deleteByOrganizationId(organizationId)

    // both of the inserted domains should be deleted
    val emailDomains = organizationEmailDomainRepository.findByEmailDomainIgnoreCase("airbyte.io")
    assert(emailDomains.isEmpty())

    val otherEmailDomains = organizationEmailDomainRepository.findByEmailDomainIgnoreCase("airbyte.com")
    assert(otherEmailDomains.isEmpty())
  }

  @Test
  fun `test find by organization id and email domain`() {
    val organizationId = UUID.randomUUID()
    val orgEmailDomain =
      OrganizationEmailDomain(
        organizationId = organizationId,
        emailDomain = "airbyte.io",
      )
    organizationEmailDomainRepository.save(orgEmailDomain)

    // Should find the domain
    val found = organizationEmailDomainRepository.findByOrganizationIdAndEmailDomain(organizationId, "airbyte.io")
    assert(found != null)
    assert(found!!.organizationId == organizationId)
    assert(found.emailDomain == "airbyte.io")

    // Should not find with wrong organization
    val notFound = organizationEmailDomainRepository.findByOrganizationIdAndEmailDomain(UUID.randomUUID(), "airbyte.io")
    assert(notFound == null)

    // Should not find with wrong domain
    val notFoundDomain = organizationEmailDomainRepository.findByOrganizationIdAndEmailDomain(organizationId, "other.io")
    assert(notFoundDomain == null)
  }
}
