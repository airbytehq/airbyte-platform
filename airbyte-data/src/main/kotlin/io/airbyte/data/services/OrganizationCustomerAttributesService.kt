package io.airbyte.data.services

import java.util.UUID

enum class CustomerTier {
  TIER_0,
  TIER_1,
  TIER_2,
}

/**
 * A service that reads organization tier information from GCS.
 */
interface OrganizationCustomerAttributesService {
  /**
   * Get a map of organization ID to customer tier.
   */
  fun getOrganizationTiers(): Map<UUID, CustomerTier?>
}
