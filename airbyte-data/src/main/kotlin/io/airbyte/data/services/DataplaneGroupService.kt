/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.config.DataplaneGroup
import java.util.UUID

interface DataplaneGroupService {
  /**
   * Get a dataplane group by its id.
   */
  fun getDataplaneGroup(id: UUID): DataplaneGroup

  /**
   * Get the dataplane group by organization ID and name.
   */
  fun getDataplaneGroupByOrganizationIdAndName(
    organizationId: UUID,
    name: String,
  ): DataplaneGroup

  /**
   * Write (create or update) a dataplane group.
   */
  fun writeDataplaneGroup(dataplaneGroup: DataplaneGroup): DataplaneGroup

  /**
   * List all dataplane groups matching the provided organization ID
   */
  fun listDataplaneGroups(
    organizationIds: List<UUID>,
    withTombstone: Boolean,
  ): List<DataplaneGroup>

  /**
   * Get the default dataplane group
   */
  fun getDefaultDataplaneGroup(): DataplaneGroup

  fun getOrganizationIdFromDataplaneGroup(dataplaneGroupId: UUID): UUID

  /**
   * List all default dataplane groups that are available for general use.
   */
  fun listDefaultDataplaneGroups(): List<DataplaneGroup> = listDataplaneGroups(listOf(DEFAULT_ORGANIZATION_ID), false)
}
