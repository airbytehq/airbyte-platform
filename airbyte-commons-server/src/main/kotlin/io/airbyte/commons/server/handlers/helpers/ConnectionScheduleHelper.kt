/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import com.cronutils.model.Cron
import io.airbyte.api.model.generated.ConnectionScheduleData
import io.airbyte.api.model.generated.ConnectionScheduleType
import io.airbyte.api.problems.model.generated.ProblemCronData
import io.airbyte.api.problems.model.generated.ProblemCronExpressionData
import io.airbyte.api.problems.model.generated.ProblemCronTimezoneData
import io.airbyte.api.problems.throwable.generated.CronValidationInvalidExpressionProblem
import io.airbyte.api.problems.throwable.generated.CronValidationInvalidTimezoneProblem
import io.airbyte.api.problems.throwable.generated.CronValidationMissingComponentProblem
import io.airbyte.api.problems.throwable.generated.CronValidationMissingCronProblem
import io.airbyte.api.problems.throwable.generated.CronValidationUnderOneHourNotAllowedProblem
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.PlatformSubOneHourSyncFrequency
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.helpers.CronExpressionHelper
import io.airbyte.config.BasicSchedule
import io.airbyte.config.Schedule
import io.airbyte.config.ScheduleData
import io.airbyte.config.StandardSync
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.SubOneHourSyncSchedules
import io.airbyte.featureflag.Workspace
import io.airbyte.persistence.job.WorkspaceHelper
import io.airbyte.validation.json.JsonValidationException
import jakarta.inject.Singleton
import org.joda.time.DateTimeZone
import org.quartz.CronExpression
import java.text.ParseException
import java.util.List
import java.util.Locale

/**
 * Helper class to handle connection schedules, including validation and translating between API and
 * config.
 */
@Singleton
class ConnectionScheduleHelper(
  private val apiPojoConverters: ApiPojoConverters,
  private val cronExpressionHelper: CronExpressionHelper,
  private val featureFlagClient: FeatureFlagClient,
  private val entitlementService: EntitlementService,
  private val workspaceHelper: WorkspaceHelper,
) {
  /**
   * Populate schedule data into a standard sync. Mutates the input object!
   *
   * @param standardSync sync to hydrate
   * @param scheduleType schedule type to add to sync
   * @param scheduleData schedule data to add to sync
   * @throws JsonValidationException exception if any of the inputs are invalid json
   */
  @Throws(JsonValidationException::class, ConfigNotFoundException::class)
  fun populateSyncFromScheduleTypeAndData(
    standardSync: StandardSync,
    scheduleType: ConnectionScheduleType,
    scheduleData: ConnectionScheduleData?,
  ) {
    if (scheduleType != ConnectionScheduleType.MANUAL && scheduleData == null) {
      throw JsonValidationException("schedule data must be populated if schedule type is populated")
    }
    when (scheduleType) {
      ConnectionScheduleType.MANUAL -> {
        standardSync.withScheduleType(StandardSync.ScheduleType.MANUAL).withScheduleData(null).withManual(true)

        // explicitly null out the legacy `schedule` column until it's removed.
        standardSync.withSchedule(null)
      }

      ConnectionScheduleType.BASIC -> {
        if (scheduleData?.basicSchedule == null) {
          throw JsonValidationException("if schedule type is basic, then scheduleData.basic must be populated")
        }
        standardSync
          .withScheduleType(StandardSync.ScheduleType.BASIC_SCHEDULE)
          .withScheduleData(
            ScheduleData().withBasicSchedule(
              BasicSchedule()
                .withTimeUnit(apiPojoConverters.toBasicScheduleTimeUnit(scheduleData.basicSchedule.timeUnit))
                .withUnits(scheduleData.basicSchedule.units),
            ),
          ).withManual(false)
        // Populate the legacy format for now as well, since some places still expect it to exist.
        // TODO(https://github.com/airbytehq/airbyte/issues/11432): remove.
        val schedule =
          Schedule()
            .withTimeUnit(apiPojoConverters.toLegacyScheduleTimeUnit(scheduleData.basicSchedule.timeUnit))
            .withUnits(scheduleData.basicSchedule.units)
        standardSync
          .withManual(false)
          .withSchedule(schedule)
      }

      ConnectionScheduleType.CRON -> {
        val connectionId = if (standardSync.connectionId == null) null else standardSync.connectionId.toString()
        if (scheduleData?.cron == null) {
          throw CronValidationMissingCronProblem()
        }
        val cronExpression = scheduleData.cron.cronExpression
        val cronTimeZone = scheduleData.cron.cronTimeZone
        if (cronExpression == null || cronTimeZone == null) {
          throw CronValidationMissingComponentProblem(
            ProblemCronData()
              .connectionId(connectionId)
              .cronExpression(cronExpression)
              .cronTimezone(cronTimeZone),
          )
        }

        val workspaceId = workspaceHelper.getWorkspaceForSourceId(standardSync.sourceId)
        val organizationId = workspaceHelper.getOrganizationForWorkspace(workspaceId)
        val canSyncUnderOneHour =
          featureFlagClient.boolVariation(
            SubOneHourSyncSchedules,
            Multi(
              List.of(Organization(organizationId), Workspace(workspaceId)),
            ),
          ) ||
            entitlementService.checkEntitlement(organizationId, PlatformSubOneHourSyncFrequency).isEntitled

        validateCronFrequency(cronExpression, canSyncUnderOneHour)

        validateCronExpressionAndTimezone(cronTimeZone, cronExpression, connectionId)

        standardSync
          .withScheduleType(StandardSync.ScheduleType.CRON)
          .withScheduleData(
            ScheduleData().withCron(
              io.airbyte.config
                .Cron()
                .withCronExpression(cronExpression)
                .withCronTimeZone(cronTimeZone),
            ),
          ).withManual(false)

        // explicitly null out the legacy `schedule` column until it's removed.
        standardSync.withSchedule(null)
      }

      else -> {}
    }
  }

  private fun validateCronExpressionAndTimezone(
    cronTimeZone: String,
    cronExpression: String,
    connectionId: String?,
  ) {
    if (cronTimeZone.lowercase(Locale.getDefault()).startsWith("etc")) {
      throw CronValidationInvalidTimezoneProblem(
        ProblemCronTimezoneData()
          .connectionId(connectionId)
          .cronTimezone(cronTimeZone),
      )
    }

    try {
      val timeZone = DateTimeZone.forID(cronTimeZone).toTimeZone()
      val parsedCronExpression = CronExpression(cronExpression)
      parsedCronExpression.timeZone = timeZone
    } catch (e: ParseException) {
      throw CronValidationInvalidExpressionProblem(
        ProblemCronExpressionData()
          .cronExpression(cronExpression),
      )
    } catch (e: IllegalArgumentException) {
      throw CronValidationInvalidTimezoneProblem(
        ProblemCronTimezoneData()
          .connectionId(connectionId)
          .cronTimezone(cronTimeZone),
      )
    }
  }

  private fun validateCronFrequency(
    cronExpression: String,
    canSyncUnderOneHour: Boolean,
  ) {
    val cronUtilsModel: Cron

    try {
      cronUtilsModel = cronExpressionHelper.validateCronExpression(cronExpression)
    } catch (e: IllegalArgumentException) {
      throw CronValidationInvalidExpressionProblem(
        ProblemCronExpressionData()
          .cronExpression(cronExpression),
      )
    }

    try {
      if (!canSyncUnderOneHour) {
        cronExpressionHelper.checkDoesNotExecuteMoreThanOncePerHour(cronUtilsModel)
      }
    } catch (e: IllegalArgumentException) {
      throw CronValidationUnderOneHourNotAllowedProblem(
        ProblemCronExpressionData()
          .cronExpression(cronExpression)
          .validationErrorMessage(e.message),
      )
    }
  }
}
