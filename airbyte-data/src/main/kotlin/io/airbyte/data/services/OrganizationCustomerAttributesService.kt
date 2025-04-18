/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.CustomerTier
import java.util.UUID

/**
 * A service that reads organization tier information from GCS.
 */
interface OrganizationCustomerAttributesService {
  /**
   * Get a map of organization ID to customer tier.
   */
  fun getOrganizationTiers(): Map<UUID, CustomerTier?>
}
