/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.config.Cron
import io.airbyte.config.ScheduleData
import io.airbyte.config.helpers.CronExpressionHelper
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.shared.ConnectionCronSchedule
import io.airbyte.domain.models.OrganizationId
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

/**
 * Unit tests for ConnectionEntitlementHelper.
 *
 * Note: These tests treat OrganizationId as a UUID for simplicity.
 * The wrapper conversion is handled in the actual implementation.
 */
class EntitlementHelperTest {
  private lateinit var entitlementService: EntitlementService
  private lateinit var connectionService: ConnectionService
  private lateinit var sourceService: SourceService
  private lateinit var destinationService: DestinationService
  private lateinit var cronExpressionHelper: CronExpressionHelper
  private lateinit var entitlementHelper: EntitlementHelper

  @BeforeEach
  fun setUp() {
    entitlementService = mockk()
    connectionService = mockk()
    sourceService = mockk()
    destinationService = mockk()
    cronExpressionHelper = CronExpressionHelper()
    entitlementHelper =
      EntitlementHelper(connectionService, cronExpressionHelper)
  }

  @Test
  fun findSubHourSyncIds() {
    val orgId = OrganizationId(UUID.randomUUID())

    val cronTimezoneUtc = "UTC"
    val cronSlow = "0 0 */4 * * ?"
    val cronSlow2 = "0 0 */2 * * ?"
    val cronFast = "0 */30 * * * ?"
    val cronFast2 = "0 */14 * * * ?"
    val invalidCron = "0 0 0 0"

    val connectionIds =
      listOf(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID())
    every { connectionService.listSubHourConnectionIdsForOrganization(orgId.value) } returns listOf(connectionIds[0], connectionIds[1])
    every { connectionService.listConnectionCronSchedulesForOrganization(orgId.value) } returns
      listOf(
        ConnectionCronSchedule(connectionIds[2], ScheduleData().withCron(Cron().withCronExpression(cronSlow).withCronTimeZone(cronTimezoneUtc))),
        ConnectionCronSchedule(connectionIds[3], ScheduleData().withCron(Cron().withCronExpression(cronFast).withCronTimeZone(cronTimezoneUtc))),
        ConnectionCronSchedule(connectionIds[4], ScheduleData().withCron(Cron().withCronExpression(cronSlow2).withCronTimeZone(cronTimezoneUtc))),
        ConnectionCronSchedule(connectionIds[5], ScheduleData().withCron(Cron().withCronExpression(cronFast2).withCronTimeZone(cronTimezoneUtc))),
        ConnectionCronSchedule(connectionIds[6], ScheduleData().withCron(Cron().withCronExpression(invalidCron).withCronTimeZone(cronTimezoneUtc))),
      )

    val result = entitlementHelper.findSubHourSyncIds(orgId)

    assertTrue(result.containsAll(listOf(connectionIds[0], connectionIds[1], connectionIds[3], connectionIds[5])))
  }
}
