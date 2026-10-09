/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.fusion

import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.domain.models.OrganizationId
import jakarta.inject.Singleton
import java.util.Optional

@Singleton
open class OrganizationSemanticSearchService(
  private val organizationRepository: OrganizationRepository,
) {
  /** Returns empty when no active organization has this ID. */
  open fun getEnablement(organizationId: OrganizationId): Optional<Boolean> =
    organizationRepository.findSemanticSearchEnabledById(organizationId.value)

  /** Returns false when no active organization has this ID. */
  open fun updateEnablement(
    organizationId: OrganizationId,
    semanticSearchEnabled: Boolean,
  ): Boolean = organizationRepository.updateSemanticSearchEnabledById(organizationId.value, semanticSearchEnabled) > 0
}
