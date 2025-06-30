import { z } from "zod";

import { DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";

// This allows us to programatically set the sync mode back to null, but still validates that a user selects a sync mode
// before submitting the form.
const noSourceSyncModeSelected = z.object({
  sourceSyncMode: z.null().refine((val) => val !== null, {
    message: "form.empty.error",
  }),
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
});

const destinationAppendSyncMode = z.object({
  destinationSyncMode: z.literal(DestinationSyncMode.append),
  matchingKeys: z.null(),
});

const destinationAppendDedupSyncMode = z.object({
  destinationSyncMode: z.literal(DestinationSyncMode.append_dedup),
  matchingKeys: z.array(z.string()).optional(),
});

const destinationUpdateSyncMode = z.object({
  destinationSyncMode: z.literal(DestinationSyncMode.update),
  matchingKeys: z.array(z.string()).optional(),
});

const destinationSoftDeleteSyncMode = z.object({
  destinationSyncMode: z.literal(DestinationSyncMode.soft_delete),
  matchingKeys: z.array(z.string()).optional(),
});

const destinationSyncMode = z.discriminatedUnion("destinationSyncMode", [
  noDestinationSyncModeSelected,
  destinationAppendSyncMode,
  destinationAppendDedupSyncMode,
  destinationUpdateSyncMode,
  destinationSoftDeleteSyncMode,
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
  .and(destinationSyncMode);

export const DataActivationConnectionFormSchema = z.object({
  streams: z.array(DataActivationStreamSchema),
});

export type DataActivationConnectionFormOutput = z.infer<typeof DataActivationConnectionFormSchema>;
