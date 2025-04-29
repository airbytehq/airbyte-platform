/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.Dataplane
import java.util.UUID

interface DataplaneService {
  /**
   * Get a dataplane by its id.
   */
  fun getDataplane(id: UUID): Dataplane

  /**
   * Write (create or update) a dataplane.
   */
  fun writeDataplane(dataplane: Dataplane): Dataplane

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
}
