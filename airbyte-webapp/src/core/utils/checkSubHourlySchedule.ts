import { useCallback } from "react";

import { FormConnectionFormValues } from "components/connection/ConnectionForm/formConfig";

import { useDescribeCronExpressionFetchQuery } from "core/api";
import { ConnectionScheduleType } from "core/api/types/AirbyteClient";

import { isMoreFrequentThanHourlyFromExecutions } from "./cron/cronFrequency";

export interface CheckSubHourlyScheduleParams {
  scheduleType: ConnectionScheduleType;
  scheduleData: FormConnectionFormValues["scheduleData"];
}

/**
 * Hook that returns a function to check if a connection schedule is sub-hourly.
 * Handles both basic schedules and cron expressions.
 *
 * @returns Function that checks if schedule is sub-hourly and returns Promise<boolean>
 */
export const useCheckSubHourlySchedule = () => {
  const fetchCronDescription = useDescribeCronExpressionFetchQuery();

  return useCallback(
    async ({ scheduleType, scheduleData }: CheckSubHourlyScheduleParams): Promise<boolean> => {
      // Check for sub-hourly basic schedule
      if (scheduleType === ConnectionScheduleType.basic) {
        const basicSchedule = scheduleData?.basicSchedule;

        if (basicSchedule?.timeUnit === "minutes") {
          return (basicSchedule.units ?? 0) < 60;
        }

        return false;
      }

      // Check for sub-hourly cron schedule
      if (scheduleType === ConnectionScheduleType.cron) {
        const cronExpression = scheduleData?.cron?.cronExpression;

        if (cronExpression) {
          const cronValidationResult = await fetchCronDescription(cronExpression);
          return (
            cronValidationResult.isValid && isMoreFrequentThanHourlyFromExecutions(cronValidationResult.nextExecutions)
          );
        }
      }

      return false;
    },
    [fetchCronDescription]
  );
};
