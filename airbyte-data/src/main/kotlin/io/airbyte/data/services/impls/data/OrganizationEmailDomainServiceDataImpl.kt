/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.OrganizationEmailDomain
import io.airbyte.data.repositories.OrganizationEmailDomainRepository
import io.airbyte.data.services.OrganizationEmailDomainService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.UUID

private val logger = KotlinLogging.logger { }

@Singleton
class OrganizationEmailDomainServiceDataImpl(
  private val repository: OrganizationEmailDomainRepository,
) : OrganizationEmailDomainService {
  override fun findByEmailDomain(emailDomain: String): List<OrganizationEmailDomain> =
    repository.findByEmailDomainIgnoreCase(emailDomain).map { it.toConfigModel() }

  override fun createEmailDomain(emailDomainConfig: OrganizationEmailDomain) {
    repository.save(
      io.airbyte.data.repositories.entities.OrganizationEmailDomain(
        organizationId = emailDomainConfig.organizationId,
        emailDomain = emailDomainConfig.emailDomain,
      ),
    )
  }

  override fun deleteAllEmailDomains(organizationId: UUID) = repository.deleteByOrganizationId(organizationId)

  override fun findByOrganizationId(organizationId: UUID): List<OrganizationEmailDomain> =
    repository.findByOrganizationId(organizationId).map {
      it.toConfigModel()
    }

  override fun existsByOrganizationIdAndDomain(
    organizationId: UUID,
    domain: String,
  ): Boolean = repository.existsByOrganizationIdAndEmailDomain(organizationId, domain)

  override fun deleteByOrganizationIdAndDomain(
    organizationId: UUID,
    domain: String,
  ) {
    val emailDomainRecord = repository.findByOrganizationIdAndEmailDomain(organizationId, domain)
    if (emailDomainRecord != null) {
      repository.deleteById(emailDomainRecord.id!!)
      logger.info { "Deleted email domain enforcement for $domain in organization $organizationId" }
    } else {
      logger.debug {
        "No email domain enforcement found for $domain in organization $organizationId (may not have been verified or SSO not active)"
      }
    }
  }
}
