/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Organization
import io.airbyte.data.repositories.entities.OrganizationEmailDomain
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
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
    organizationRepository.deleteAll()
  }

  private fun createOrganizationId(tombstone: Boolean = false): UUID =
    organizationRepository
      .save(
        Organization(
          name = "Test Organization",
          email = "test-${UUID.randomUUID()}@example.com",
          userId = UUID.randomUUID(),
          tombstone = tombstone,
        ),
      ).id!!

  @Test
  fun `test db insertion`() {
    val organizationId = UUID.randomUUID()
    val orgEmailDomain =
      OrganizationEmailDomain(
        organizationId = organizationId,
        emailDomain = "airbyte.io",
      )

    val countBeforeSave = organizationEmailDomainRepository.count()
    assertEquals(0L, countBeforeSave)

    val saveResult = organizationEmailDomainRepository.save(orgEmailDomain)
    val countAfterSave = organizationEmailDomainRepository.count()
    assertEquals(1L, countAfterSave)

    val persistedOrgEmailDomain = organizationEmailDomainRepository.findById(saveResult.id!!).get()
    assertEquals(organizationId, persistedOrgEmailDomain.organizationId)
    assertEquals("airbyte.io", persistedOrgEmailDomain.emailDomain)
  }

  @Test
  fun `test find by email domain`() {
    assertTrue(organizationEmailDomainRepository.findByEmailDomainIgnoreCase("airbyte.io").isEmpty())

    val organizationId = createOrganizationId()
    val orgEmailDomain =
      OrganizationEmailDomain(
        organizationId = organizationId,
        emailDomain = "airbyte.io",
      )

    organizationEmailDomainRepository.save(orgEmailDomain)

    val otherOrgId = createOrganizationId()
    val otherOrgEmailDomain =
      OrganizationEmailDomain(
        organizationId = otherOrgId,
        emailDomain = "airbyte.dev",
      )
    organizationEmailDomainRepository.save(otherOrgEmailDomain)

    val orgEmailDomains = organizationEmailDomainRepository.findByEmailDomainIgnoreCase("airbyte.io")
    assertEquals(1, orgEmailDomains.size)
    assertEquals(organizationId, orgEmailDomains[0].organizationId)
    assertEquals("airbyte.io", orgEmailDomains[0].emailDomain)
  }

  @Test
  fun `test find by email domain is case insensitive`() {
    val organizationId = createOrganizationId()
    val orgEmailDomain =
      OrganizationEmailDomain(
        organizationId = organizationId,
        emailDomain = "Airbyte.IO",
      )

    organizationEmailDomainRepository.save(orgEmailDomain)

    // Search with lowercase should find the domain stored with mixed case
    val resultLowercase = organizationEmailDomainRepository.findByEmailDomainIgnoreCase("airbyte.io")
    assertEquals(1, resultLowercase.size)
    assertEquals(organizationId, resultLowercase[0].organizationId)
    assertEquals("Airbyte.IO", resultLowercase[0].emailDomain)

    // Search with uppercase should also find it
    val resultUppercase = organizationEmailDomainRepository.findByEmailDomainIgnoreCase("AIRBYTE.IO")
    assertEquals(1, resultUppercase.size)
    assertEquals(organizationId, resultUppercase[0].organizationId)
    assertEquals("Airbyte.IO", resultUppercase[0].emailDomain)

    // Search with different mixed case should also find it
    val resultMixed = organizationEmailDomainRepository.findByEmailDomainIgnoreCase("AiRbYtE.iO")
    assertEquals(1, resultMixed.size)
    assertEquals(organizationId, resultMixed[0].organizationId)
    assertEquals("Airbyte.IO", resultMixed[0].emailDomain)
  }

  @Test
  fun `test find by email domain excludes tombstoned organizations`() {
    val tombstonedOrganizationId = createOrganizationId(tombstone = true)
    organizationEmailDomainRepository.save(
      OrganizationEmailDomain(
        organizationId = tombstonedOrganizationId,
        emailDomain = "airbyte.io",
      ),
    )

    assertTrue(organizationEmailDomainRepository.findByEmailDomainIgnoreCase("airbyte.io").isEmpty())

    val activeOrganizationId = createOrganizationId()
    organizationEmailDomainRepository.save(
      OrganizationEmailDomain(
        organizationId = activeOrganizationId,
        emailDomain = "airbyte.io",
      ),
    )

    val result = organizationEmailDomainRepository.findByEmailDomainIgnoreCase("airbyte.io")
    assertEquals(1, result.size)
    assertEquals(activeOrganizationId, result[0].organizationId)
  }

  @Test
  fun `delete by organization id removes all related rows`() {
    val organizationId = createOrganizationId()
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
    assertTrue(emailDomains.isEmpty())

    val otherEmailDomains = organizationEmailDomainRepository.findByEmailDomainIgnoreCase("airbyte.com")
    assertTrue(otherEmailDomains.isEmpty())
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
    assertNotNull(found)
    assertEquals(organizationId, found!!.organizationId)
    assertEquals("airbyte.io", found.emailDomain)

    // Should not find with wrong organization
    val notFound = organizationEmailDomainRepository.findByOrganizationIdAndEmailDomain(UUID.randomUUID(), "airbyte.io")
    assertNull(notFound)

    // Should not find with wrong domain
    val notFoundDomain = organizationEmailDomainRepository.findByOrganizationIdAndEmailDomain(organizationId, "other.io")
    assertNull(notFoundDomain)
  }
}
