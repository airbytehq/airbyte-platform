/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.config.helpers.CronExpressionHelper
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.shared.ConnectionCronSchedule
import io.airbyte.domain.models.OrganizationId
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.UUID

/**
 * Helper class for checking connection entitlements.
 */
@Singleton
class EntitlementHelper(
  private val connectionService: ConnectionService,
  private val cronExpressionHelper: CronExpressionHelper,
) {
  companion object {
    private val logger = KotlinLogging.logger {}
  }

  fun findSubHourSyncIds(organizationId: OrganizationId): Collection<UUID> {
    val fastBasicSyncIds = connectionService.listSubHourConnectionIdsForOrganization(organizationId.value)
    val cronSchedules = connectionService.listConnectionCronSchedulesForOrganization(organizationId.value)

    val fastCronSyncIds =
      cronSchedules
        .filter {
          try {
            cronExpressionHelper.executesMoreThanOncePerHour(it.scheduleData.cron.cronExpression)
          } catch (e: IllegalArgumentException) {
            logger.warn { "Invalid cron expression for connection id=${it.id}, ${e.message}" }
            false
          }
        }.map(ConnectionCronSchedule::id)

    return fastBasicSyncIds + fastCronSyncIds
  }
}
