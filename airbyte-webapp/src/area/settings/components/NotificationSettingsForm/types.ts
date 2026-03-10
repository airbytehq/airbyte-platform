import { NotificationItem, NotificationSettings, NotificationTrigger } from "core/api/types/AirbyteClient";

// TODO(https://github.com/airbytehq/hydra-issues-internal/issues/109): Remove this file when backend API is ready. After running `pnpm generate-client`:
// 1. Verify `sendOnConnectionSyncQueued` exists in `NotificationSettings`
// 2. Verify `connection_sync_queued` exists in `NotificationTrigger` enum
// 3. Delete this file and update imports to use generated types directly

/**
 * Mock extended NotificationSettings until backend API is ready.
 * Adds the `sendOnConnectionSyncQueued` field for capacity enforcement notifications.
 */
export interface ExtendedNotificationSettings extends NotificationSettings {
  sendOnConnectionSyncQueued?: NotificationItem;
}

/**
 * Mock extended NotificationTrigger until backend API is ready.
 * Adds the `connection_sync_queued` trigger for capacity enforcement notifications.
 */
export const ExtendedNotificationTrigger = {
  ...NotificationTrigger,
  connection_sync_queued: "connection_sync_queued",
} as const;

/**
 * Mock extended NotificationTriggerType until backend API is ready.
 * Union type of all notification trigger values including `connection_sync_queued`.
 */
export type ExtendedNotificationTriggerType =
  (typeof ExtendedNotificationTrigger)[keyof typeof ExtendedNotificationTrigger];
