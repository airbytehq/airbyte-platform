import { z } from "zod";

import { validateCronExpression } from "area/connection/utils/validateCronExpression";
import { ConnectionScheduleDataBasicScheduleTimeUnit, ConnectionScheduleType } from "core/api/types/AirbyteClient";
import { NON_I18N_ERROR_TYPE } from "core/utils/form";

const manualScheduleSchema = z.object({
  scheduleType: z.literal(ConnectionScheduleType.manual),
  scheduleData: z.object({}).optional(),
});

const basicScheduleSchema = z.object({
  scheduleType: z.literal(ConnectionScheduleType.basic),
  scheduleData: z.object({
    basicSchedule: z.object({
      units: z.number(),
      timeUnit: z.nativeEnum(ConnectionScheduleDataBasicScheduleTimeUnit),
    }),
  }),
});

const cronScheduleSchema = z.object({
  scheduleType: z.literal(ConnectionScheduleType.cron),
  scheduleData: z.object({
    cron: z.object({
      cronExpression: z
        .string()
        .trim()
        .superRefine(async (value, ctx) => {
          if (!value) {
            ctx.addIssue({
              code: z.ZodIssueCode.custom,
              message: "form.empty.error",
            });
            return;
          }

          try {
            const response = validateCronExpression(value);
            if (!response.isValid) {
              ctx.addIssue({
                code: z.ZodIssueCode.custom,
                message: response.message,
                params: { type: NON_I18N_ERROR_TYPE },
              });
            }
          } catch (error) {
            ctx.addIssue({
              code: z.ZodIssueCode.custom,
              message: error.message,
            });
          }
        }),
      cronTimeZone: z.string().nonempty("form.empty.error"),
    }),
  }),
});

export const scheduleDataSchema = z.discriminatedUnion("scheduleType", [
  manualScheduleSchema,
  basicScheduleSchema,
  cronScheduleSchema,
]);
