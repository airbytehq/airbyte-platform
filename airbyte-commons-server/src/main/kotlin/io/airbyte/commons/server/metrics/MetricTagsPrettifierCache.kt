/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.metrics

import io.airbyte.config.DataplaneGroup
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneService
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.cache.annotation.Cacheable
import jakarta.inject.Singleton
import java.util.UUID

val logger = KotlinLogging.logger {}

/**
 * Caching Dataplane and Dataplane Group names for use in metrics.
 */
@Singleton
open class MetricTagsPrettifierCache(
  private val dataplaneService: DataplaneService,
  private val dataplaneGroupService: DataplaneGroupService,
) {
  @Cacheable("dataplaneName")
  open fun dataplaneNameById(dataplaneId: UUID): String =
    try {
      dataplaneService.getDataplane(dataplaneId).name
    } catch (e: Exception) {
      logger.warn(e) { "Error retrieving Dataplane name for Dataplane ID: $dataplaneId" }
      dataplaneId.toString()
    }

  fun dataplaneGroupNameById(dataplaneGroupId: UUID): String = dataplaneGroupById(dataplaneGroupId)?.name ?: dataplaneGroupId.toString()

  fun orgIdForDataplaneGroupId(dataplaneGroupId: UUID): UUID? = dataplaneGroupById(dataplaneGroupId)?.organizationId

  @Cacheable("dataplaneGroup")
  open fun dataplaneGroupById(dataplaneGroupId: UUID): DataplaneGroup? =
    try {
      dataplaneGroupService.getDataplaneGroup(dataplaneGroupId)
    } catch (e: Exception) {
      logger.warn(e) { "Error retrieving Dataplane Group name for Dataplane Group ID: $dataplaneGroupId" }
      null
    }
}
