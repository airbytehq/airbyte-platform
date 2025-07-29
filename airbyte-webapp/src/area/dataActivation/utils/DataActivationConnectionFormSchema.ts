import { z } from "zod";

import { DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";

// This allows us to programatically set the sync mode back to null, but still validates that a user selects a sync mode
// before submitting the form.
const noSourceSyncModeSelected = z.object({
  sourceSyncMode: z.null().refine((val) => val !== null, {
    message: "form.empty.error",
  }),
  cursorField: z.null(),
});

const incrementalSyncMode = z.object({
  sourceSyncMode: z.literal(SyncMode.incremental),
  cursorField: z.string().nonempty("form.empty.error"),
});

const fullRefreshSyncMode = z.object({
  sourceSyncMode: z.literal(SyncMode.full_refresh),
  cursorField: z.null(),
});

const sourceSyncMode = z.discriminatedUnion("sourceSyncMode", [
  noSourceSyncModeSelected,
  incrementalSyncMode,
  fullRefreshSyncMode,
]);

// This allows us to programatically set the destination sync mode back to null, but still validates that a user selects
// a destination sync mode before submitting the form.
const noDestinationSyncModeSelected = z.object({
  destinationSyncMode: z.null().refine((val) => val !== null, {
    message: "form.empty.error",
  }),
  matchingKeys: z.null(),
});

const AllowedDestinationSyncModes = z.enum([
  DestinationSyncMode.append,
  DestinationSyncMode.append_dedup,
  DestinationSyncMode.update,
  DestinationSyncMode.soft_delete,
]);

const someDestinationSyncMode = z.object({
  destinationSyncMode: AllowedDestinationSyncModes,
  matchingKeys: z.array(z.string()).nullable(),
});

const destinationSyncMode = z.discriminatedUnion("destinationSyncMode", [
  noDestinationSyncModeSelected,
  someDestinationSyncMode,
]);

const DataActivationStreamSchema = z
  .object({
    sourceStreamDescriptor: z
      .object({
        name: z.string(),
        namespace: z.string().optional(),
      })
      .refine((s) => !!s.name && s.name.trim().length > 0, {
        message: "form.empty.error",
        path: [],
      }),
    destinationObjectName: z.string().trim().nonempty("form.empty.error"),
    fields: z.array(
      z.object({
        sourceFieldName: z.string().nonempty("form.empty.error"),
        destinationFieldName: z.string().nonempty("form.empty.error"),
      })
    ),
  })
  .and(sourceSyncMode)
  .and(destinationSyncMode)
  // Validate that each destination field name is unique
  .superRefine((stream, ctx) => {
    const seen = new Set<string>();
    stream.fields.forEach((field, i) => {
      const name = field.destinationFieldName;
      if (seen.has(name)) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: "Duplicate destinationFieldName",
          path: ["fields", i, "destinationFieldName"],
        });
      } else {
        seen.add(name);
      }
    });
  });

export const DataActivationConnectionFormSchema = z.object({
  streams: z.array(DataActivationStreamSchema),
});

export type DataActivationConnectionFormOutput = z.infer<typeof DataActivationConnectionFormSchema>;
