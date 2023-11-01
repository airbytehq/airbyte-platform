import React from "react";
import { useWatch } from "react-hook-form";

import { ConnectionScheduleType } from "core/request/AirbyteClient";

import { BasicScheduleFormControl } from "./BasicScheduleFormControl";
import { CronScheduleFormControl } from "./CronScheduleFormControl";
import { ScheduleTypeFormControl } from "./ScheduleTypeFormControl";

export const ScheduleHookFormField: React.FC = () => {
  const watchedScheduleType = useWatch({ name: "scheduleType" });

  return (
    <>
      <ScheduleTypeFormControl />
      {watchedScheduleType === ConnectionScheduleType.basic && <BasicScheduleFormControl />}
      {watchedScheduleType === ConnectionScheduleType.cron && <CronScheduleFormControl />}
    </>
  );
};
