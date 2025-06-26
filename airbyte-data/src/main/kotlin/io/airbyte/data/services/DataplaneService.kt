/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.Dataplane
import io.airbyte.data.services.shared.DataplaneWithServiceAccount
import java.util.UUID

interface DataplaneService {
  /**
   * Get a dataplane by its id.
   */
  fun getDataplane(id: UUID): Dataplane

  /**
   * Update a dataplane.
   */
  fun updateDataplane(dataplane: Dataplane): Dataplane

  /**
   * Create a new dataplane. This also creates a service account for the dataplane
   * and returns the account's credentials.
   */
  fun createDataplaneAndServiceAccount(
    dataplane: Dataplane,
    instanceScope: Boolean = false,
  ): DataplaneWithServiceAccount

  /**
   * List all dataplanes matching the provided dataplane group ID
   */
  fun listDataplanes(
    dataplaneGroupId: UUID,
    withTombstone: Boolean,
  ): List<Dataplane>

  /**
   * List all dataplanes
   */
  fun listDataplanes(withTombstone: Boolean): List<Dataplane>

  /**
   * Get a dataplane by its service account.
   */
  fun getDataplaneByServiceAccountId(serviceAccountId: String): Dataplane?
}
