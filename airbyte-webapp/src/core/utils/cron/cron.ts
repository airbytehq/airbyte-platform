import cronstrue from "cronstrue";

import { ConnectionScheduleDataCron } from "core/api/types/AirbyteClient";

import availableCronTimeZones from "./availableCronTimeZones.json";

/**
 * Turns a cron expression into a human readable string.
 */
export const humanizeCron = (cronExpression: string) => {
  return cronstrue.toString(cronExpression, { dayOfWeekStartIndexZero: false });
};

export const CRON_DEFAULT_VALUE: ConnectionScheduleDataCron = {
  cronTimeZone: "UTC",
  // Fire at 12:00 PM (noon) every day
  cronExpression: "0 0 12 * * ?",
};

export const cronTimeZones = availableCronTimeZones.map((zone: string) => ({ label: zone, value: zone }));
