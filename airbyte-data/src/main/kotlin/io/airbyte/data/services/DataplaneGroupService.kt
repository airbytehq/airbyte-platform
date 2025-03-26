/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.DataplaneGroup
import java.util.UUID

interface DataplaneGroupService {
  /**
   * Get a dataplane group by its id.
   */
  fun getDataplaneGroup(id: UUID): DataplaneGroup

  /**
   * Get the dataplane group by geography .
   */
  fun getDataplaneGroupByOrganizationIdAndGeography(
    organizationId: UUID,
    geography: String,
  ): DataplaneGroup

  /**
   * Write (create or update) a dataplane group.
   */
  fun writeDataplaneGroup(dataplaneGroup: DataplaneGroup): DataplaneGroup

  /**
   * List all dataplane groups matching the provided organization ID
   */
  fun listDataplaneGroups(
    organizationId: UUID,
    withTombstone: Boolean,
  ): List<DataplaneGroup>
}
