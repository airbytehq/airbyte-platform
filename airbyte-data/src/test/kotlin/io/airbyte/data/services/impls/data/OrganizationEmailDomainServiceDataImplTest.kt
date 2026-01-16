/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.repositories.OrganizationEmailDomainRepository
import io.airbyte.data.repositories.entities.OrganizationEmailDomain
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class OrganizationEmailDomainServiceDataImplTest {
  private val organizationEmailDomainRepository = mockk<OrganizationEmailDomainRepository>()
  private val organizationEmailDomainServiceDataImpl = OrganizationEmailDomainServiceDataImpl(organizationEmailDomainRepository)

  @BeforeEach
  fun reset() {
    clearAllMocks()
  }

  @Test
  fun `test findByEmailDomain`() {
    val id = UUID.randomUUID()
    val orgId = UUID.randomUUID()
    val orgEmailDomains =
      listOf(
        OrganizationEmailDomain(
          id = id,
          organizationId = orgId,
          emailDomain = "airbyte.io",
        ),
      )

    every { organizationEmailDomainRepository.findByEmailDomainIgnoreCase("airbyte.io") } returns orgEmailDomains

    val result = organizationEmailDomainServiceDataImpl.findByEmailDomain("airbyte.io")
    assert(result.size == 1)

    val expectedOrgEmailDomain =
      io.airbyte.config
        .OrganizationEmailDomain()
        .withId(id)
        .withEmailDomain("airbyte.io")
        .withOrganizationId(orgId)

    assert(result[0] == expectedOrgEmailDomain)
  }

  @Test
  fun `test findByEmailDomain is case insensitive`() {
    val id = UUID.randomUUID()
    val orgId = UUID.randomUUID()
    val orgEmailDomains =
      listOf(
        OrganizationEmailDomain(
          id = id,
          organizationId = orgId,
          emailDomain = "Airbyte.IO",
        ),
      )

    // Repository should find the domain regardless of case
    every { organizationEmailDomainRepository.findByEmailDomainIgnoreCase("airbyte.io") } returns orgEmailDomains

    val result = organizationEmailDomainServiceDataImpl.findByEmailDomain("airbyte.io")
    assert(result.size == 1)
    assert(result[0].emailDomain == "Airbyte.IO")
  }

  @Test
  fun `test deleteByOrganizationIdAndDomain when record exists`() {
    val orgId = UUID.randomUUID()
    val id = UUID.randomUUID()
    val domain = "airbyte.io"

    val orgEmailDomain =
      OrganizationEmailDomain(
        id = id,
        organizationId = orgId,
        emailDomain = domain,
      )

    every { organizationEmailDomainRepository.findByOrganizationIdAndEmailDomain(orgId, domain) } returns orgEmailDomain
    every { organizationEmailDomainRepository.deleteById(id) } returns Unit

    organizationEmailDomainServiceDataImpl.deleteByOrganizationIdAndDomain(orgId, domain)

    verify { organizationEmailDomainRepository.findByOrganizationIdAndEmailDomain(orgId, domain) }
    verify { organizationEmailDomainRepository.deleteById(id) }
  }

  @Test
  fun `test deleteByOrganizationIdAndDomain when record does not exist`() {
    val orgId = UUID.randomUUID()
    val domain = "airbyte.io"

    every { organizationEmailDomainRepository.findByOrganizationIdAndEmailDomain(orgId, domain) } returns null

    organizationEmailDomainServiceDataImpl.deleteByOrganizationIdAndDomain(orgId, domain)

    verify { organizationEmailDomainRepository.findByOrganizationIdAndEmailDomain(orgId, domain) }
    verify(exactly = 0) { organizationEmailDomainRepository.deleteById(any()) }
  }
}
