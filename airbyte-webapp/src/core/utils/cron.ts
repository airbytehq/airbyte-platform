import cronstrue from "cronstrue";

/**
 * Turns a cron expression into a human readable string.
 */
export const humanizeCron = (cronExpression: string) => {
  return cronstrue.toString(cronExpression, { dayOfWeekStartIndexZero: false });
};
