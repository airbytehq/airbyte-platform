import React from "react";
import { FormattedMessage } from "react-intl";

import { ConnectionScheduleData, ConnectionScheduleType } from "core/api/types/AirbyteClient";
import { humanizeCron } from "core/utils/cron";

export interface FormattedScheduleDataMessageProps {
  scheduleType?: ConnectionScheduleType;
  scheduleData?: ConnectionScheduleData;
}

/**
 * Formats schedule data based on the schedule type and schedule data.
 * If schedule type is "manual" returns "Manual".
 * If schedule type is "basic" returns "Every {units} {timeUnit}".
 * If schedule type is "cron" returns humanized cron expression.
 * @param scheduleType
 * @param scheduleData
 */
export const FormattedScheduleDataMessage: React.FC<FormattedScheduleDataMessageProps> = ({
  scheduleType,
  scheduleData,
}: {
  scheduleType?: ConnectionScheduleType;
  scheduleData?: ConnectionScheduleData;
}) => {
  if (scheduleType === "manual") {
    return <FormattedMessage id="frequency.manual" />;
  }

  if (scheduleType === "basic" && scheduleData?.basicSchedule) {
    return (
      <FormattedMessage
        id={`form.every.${scheduleData.basicSchedule.timeUnit}`}
        values={{ value: scheduleData.basicSchedule.units }}
      />
    );
  }

  if (scheduleType === "cron" && scheduleData?.cron) {
    return <>{humanizeCron(scheduleData.cron.cronExpression)}</>;
  }

  return null;
};
