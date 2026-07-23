/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.scim

import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.repositories.ScimConfigurationRepository
import io.airbyte.domain.models.scim.ScimAuthenticationException
import io.micronaut.transaction.TransactionOperations
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.sql.Connection

@Singleton
open class ScimMutationService(
  private val organizationRepository: OrganizationRepository,
  private val repository: ScimConfigurationRepository,
  @param:Named("config") private val transactions: TransactionOperations<Connection>,
) {
  open fun <T> execute(
    context: ScimAuthenticationContext,
    mutation: () -> T,
  ): T =
    transactions.executeWrite { _ ->
      if (organizationRepository.findByIdForUpdate(context.organizationId.value).isEmpty) {
        throw ScimAuthenticationException()
      }

      val configuration =
        repository.findByIdAndOrganizationIdForUpdate(
          context.configurationId,
          context.organizationId.value,
        )
      if (configuration?.enabled != true || !context.matchesTokenHash(configuration.tokenHash)) {
        throw ScimAuthenticationException()
      }

      mutation()
    }
}
