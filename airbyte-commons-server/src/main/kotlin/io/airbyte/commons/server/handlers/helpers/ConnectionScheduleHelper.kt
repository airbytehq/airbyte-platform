/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import com.cronutils.model.Cron
import io.airbyte.api.model.generated.ConnectionScheduleData
import io.airbyte.api.model.generated.ConnectionScheduleType
import io.airbyte.api.problems.model.generated.ProblemBasicScheduleData
import io.airbyte.api.problems.model.generated.ProblemCronData
import io.airbyte.api.problems.model.generated.ProblemCronExpressionData
import io.airbyte.api.problems.model.generated.ProblemCronTimezoneData
import io.airbyte.api.problems.throwable.generated.BasicScheduleValidationUnderOneHourNotAllowedProblem
import io.airbyte.api.problems.throwable.generated.CronValidationInvalidExpressionProblem
import io.airbyte.api.problems.throwable.generated.CronValidationInvalidTimezoneProblem
import io.airbyte.api.problems.throwable.generated.CronValidationMissingComponentProblem
import io.airbyte.api.problems.throwable.generated.CronValidationMissingCronProblem
import io.airbyte.api.problems.throwable.generated.CronValidationUnderOneHourNotAllowedProblem
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.FasterSyncFrequencyEntitlement
import io.airbyte.commons.entitlements.models.FifteenMinuteSyncFrequencyEntitlement
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.config.BasicSchedule
import io.airbyte.config.Schedule
import io.airbyte.config.ScheduleData
import io.airbyte.config.StandardSync
import io.airbyte.config.helpers.CronExpressionHelper
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.domain.models.OrganizationId
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.SubOneHourSyncSchedules
import io.airbyte.featureflag.Workspace
import io.airbyte.validation.json.JsonValidationException
import jakarta.inject.Singleton
import org.joda.time.DateTimeZone
import org.quartz.CronExpression
import java.text.ParseException
import java.util.Locale
import java.util.UUID

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
  companion object {
    private const val FIFTEEN_MINUTE_SYNC_FLOOR_MINUTES = 15L
  }

  /**
   * Populate schedule data into a standard sync. Mutates the input object!
   *
   * @param standardSync sync to hydrate
   * @param scheduleType schedule type to add to sync
   * @param scheduleData schedule data to add to sync
   * @throws JsonValidationException exception if any of the inputs are invalid json
   */
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

        val isSubHourSchedule =
          scheduleData.basicSchedule.timeUnit == io.airbyte.api.model.generated.ConnectionScheduleDataBasicSchedule.TimeUnitEnum.MINUTES &&
            scheduleData.basicSchedule.units < 60

        if (isSubHourSchedule) {
          val workspaceId = workspaceHelper.getWorkspaceForSourceId(standardSync.sourceId)
          val organizationId = workspaceHelper.getOrganizationForWorkspace(workspaceId)
          validateBasicSubHourSchedule(scheduleData.basicSchedule, workspaceId, organizationId)
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
        validateCronFrequency(cronExpression, minimumAllowedSubHourIntervalMinutes(workspaceId, organizationId))

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
    minimumAllowedSubHourIntervalMinutes: Long?,
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
      when (minimumAllowedSubHourIntervalMinutes) {
        null -> cronExpressionHelper.checkDoesNotExecuteMoreThanOncePerHour(cronUtilsModel)
        1L -> Unit
        else ->
          cronExpressionHelper.checkDoesNotExecuteMoreThanOncePerInterval(
            cronUtilsModel,
            minimumAllowedSubHourIntervalMinutes * 60,
            "$minimumAllowedSubHourIntervalMinutes minutes",
          )
      }
    } catch (e: IllegalArgumentException) {
      throw CronValidationUnderOneHourNotAllowedProblem(
        ProblemCronExpressionData()
          .cronExpression(cronExpression)
          .validationErrorMessage(e.message),
      )
    }
  }

  private fun validateBasicSubHourSchedule(
    basicSchedule: io.airbyte.api.model.generated.ConnectionScheduleDataBasicSchedule,
    workspaceId: UUID,
    organizationId: UUID,
  ) {
    val minimumAllowedSubHourIntervalMinutes = minimumAllowedSubHourIntervalMinutes(workspaceId, organizationId)
    val validationErrorMessage =
      when {
        minimumAllowedSubHourIntervalMinutes == null -> "Basic schedules must be at least 1 hour apart"
        basicSchedule.units < minimumAllowedSubHourIntervalMinutes ->
          "Basic schedules must be at least $minimumAllowedSubHourIntervalMinutes minutes apart"
        else -> null
      }

    if (validationErrorMessage != null) {
      throw BasicScheduleValidationUnderOneHourNotAllowedProblem(
        ProblemBasicScheduleData()
          .timeUnit(basicSchedule.timeUnit.toString())
          .units(basicSchedule.units.toInt())
          .validationErrorMessage(validationErrorMessage),
      )
    }
  }

  private fun minimumAllowedSubHourIntervalMinutes(
    workspaceId: UUID,
    organizationId: UUID,
  ): Long? {
    val context =
      Multi(
        listOf(Organization(organizationId), Workspace(workspaceId)),
      )
    if (featureFlagClient.boolVariation(SubOneHourSyncSchedules, context)) {
      return 1
    }

    val orgId = OrganizationId(organizationId)
    if (entitlementService.checkEntitlement(orgId, FasterSyncFrequencyEntitlement).isEntitled) {
      return 1
    }

    if (entitlementService.checkEntitlement(orgId, FifteenMinuteSyncFrequencyEntitlement).isEntitled) {
      return FIFTEEN_MINUTE_SYNC_FLOOR_MINUTES
    }

    return null
  }
}
