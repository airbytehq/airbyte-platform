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
    assert(organizationEmailDomainRepository.findByEmailDomain("airbyte.io").isEmpty())

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

    val orgEmailDomains = organizationEmailDomainRepository.findByEmailDomain("airbyte.io")
    assert(orgEmailDomains.size == 1)
    assert(orgEmailDomains[0].organizationId == organizationId)
    assert(orgEmailDomains[0].emailDomain == "airbyte.io")
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
    val emailDomains = organizationEmailDomainRepository.findByEmailDomain("airbyte.io")
    assert(emailDomains.isEmpty())

    val otherEmailDomains = organizationEmailDomainRepository.findByEmailDomain("airbyte.com")
    assert(otherEmailDomains.isEmpty())
  }
}
