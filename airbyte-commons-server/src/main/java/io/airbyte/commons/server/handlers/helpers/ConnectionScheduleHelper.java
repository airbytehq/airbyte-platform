/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import io.airbyte.api.model.generated.ConnectionScheduleData;
import io.airbyte.api.model.generated.ConnectionScheduleType;
import io.airbyte.api.problems.model.generated.ProblemCronData;
import io.airbyte.api.problems.model.generated.ProblemCronExpressionData;
import io.airbyte.api.problems.model.generated.ProblemCronTimezoneData;
import io.airbyte.api.problems.throwable.generated.CronValidationInvalidExpressionProblem;
import io.airbyte.api.problems.throwable.generated.CronValidationInvalidTimezoneProblem;
import io.airbyte.api.problems.throwable.generated.CronValidationMissingComponentProblem;
import io.airbyte.api.problems.throwable.generated.CronValidationMissingCronProblem;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.config.BasicSchedule;
import io.airbyte.config.Cron;
import io.airbyte.config.Schedule;
import io.airbyte.config.ScheduleData;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSync.ScheduleType;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Singleton;
import java.text.ParseException;
import java.util.TimeZone;
import org.joda.time.DateTimeZone;
import org.quartz.CronExpression;

/**
 * Helper class to handle connection schedules, including validation and translating between API and
 * config.
 */
@SuppressWarnings("PMD.PreserveStackTrace")
@Singleton
public class ConnectionScheduleHelper {

  private final ApiPojoConverters apiPojoConverters;

  public ConnectionScheduleHelper(final ApiPojoConverters apiPojoConverters) {
    this.apiPojoConverters = apiPojoConverters;
  }

  /**
   * Populate schedule data into a standard sync. Mutates the input object!
   *
   * @param standardSync sync to hydrate
   * @param scheduleType schedule type to add to sync
   * @param scheduleData schedule data to add to sync
   * @throws JsonValidationException exception if any of the inputs are invalid json
   */
  public void populateSyncFromScheduleTypeAndData(final StandardSync standardSync,
                                                  final ConnectionScheduleType scheduleType,
                                                  final ConnectionScheduleData scheduleData)
      throws JsonValidationException {
    if (scheduleType != ConnectionScheduleType.MANUAL && scheduleData == null) {
      throw new JsonValidationException("schedule data must be populated if schedule type is populated");
    }
    switch (scheduleType) {
      // NOTE: the `manual` column is marked required, so we populate it until it's removed.
      case MANUAL -> {
        standardSync.withScheduleType(ScheduleType.MANUAL).withScheduleData(null).withManual(true);

        // explicitly null out the legacy `schedule` column until it's removed.
        standardSync.withSchedule(null);
      }
      case BASIC -> {
        if (scheduleData.getBasicSchedule() == null) {
          throw new JsonValidationException("if schedule type is basic, then scheduleData.basic must be populated");
        }
        standardSync
            .withScheduleType(ScheduleType.BASIC_SCHEDULE)
            .withScheduleData(new ScheduleData().withBasicSchedule(
                new BasicSchedule().withTimeUnit(apiPojoConverters.toBasicScheduleTimeUnit(scheduleData.getBasicSchedule().getTimeUnit()))
                    .withUnits(scheduleData.getBasicSchedule().getUnits())))
            .withManual(false);
        // Populate the legacy format for now as well, since some places still expect it to exist.
        // TODO(https://github.com/airbytehq/airbyte/issues/11432): remove.
        final Schedule schedule = new Schedule()
            .withTimeUnit(apiPojoConverters.toLegacyScheduleTimeUnit(scheduleData.getBasicSchedule().getTimeUnit()))
            .withUnits(scheduleData.getBasicSchedule().getUnits());
        standardSync
            .withManual(false)
            .withSchedule(schedule);
      }
      case CRON -> {
        final String connectionId = standardSync.getConnectionId() == null ? null : standardSync.getConnectionId().toString();
        if (scheduleData.getCron() == null) {
          throw new CronValidationMissingCronProblem();
        }
        // Validate that this is a valid cron expression and timezone.
        final String cronExpression = scheduleData.getCron().getCronExpression();
        final String cronTimeZone = scheduleData.getCron().getCronTimeZone();
        if (cronExpression == null || cronTimeZone == null) {
          throw new CronValidationMissingComponentProblem(new ProblemCronData()
              .connectionId(connectionId)
              .cronExpression(cronExpression)
              .cronTimezone(cronTimeZone));
        }
        if (cronTimeZone.toLowerCase().startsWith("etc")) {
          throw new CronValidationInvalidTimezoneProblem(new ProblemCronTimezoneData()
              .connectionId(connectionId)
              .cronTimezone(cronTimeZone));
        }
        try {
          final TimeZone timeZone = DateTimeZone.forID(cronTimeZone).toTimeZone();
          final CronExpression parsedCronExpression = new CronExpression(cronExpression);
          parsedCronExpression.setTimeZone(timeZone);
        } catch (final ParseException e) {
          throw new CronValidationInvalidExpressionProblem(new ProblemCronExpressionData()
              .connectionId(connectionId)
              .cronExpression(cronExpression));
        } catch (final IllegalArgumentException e) {
          throw new CronValidationInvalidTimezoneProblem(new ProblemCronTimezoneData()
              .connectionId(connectionId)
              .cronTimezone(cronTimeZone));
        }
        standardSync
            .withScheduleType(ScheduleType.CRON)
            .withScheduleData(new ScheduleData().withCron(new Cron()
                .withCronExpression(cronExpression)
                .withCronTimeZone(cronTimeZone)))
            .withManual(false);

        // explicitly null out the legacy `schedule` column until it's removed.
        standardSync.withSchedule(null);
      }
      default -> {
        // no op
      }
    }
  }

}
