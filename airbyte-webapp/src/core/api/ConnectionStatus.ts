import {
  ConnectionSyncStatus as GeneratedConnectionSyncStatus,
  JobStatus as GeneratedJobStatus,
  WebBackendConnectionStatusCounts as GeneratedWebBackendConnectionStatusCounts,
} from "./types/AirbyteClient";

// TODO(https://github.com/airbytehq/hydra-issues-internal/issues/106): Remove when backend adds "queued" to ConnectionSyncStatus
// eslint-disable-next-line @typescript-eslint/no-redeclare -- intentional const/type pattern for enum extension
export const ConnectionSyncStatus = {
  ...GeneratedConnectionSyncStatus,
  queued: "queued" as const,
};

// eslint-disable-next-line @typescript-eslint/no-redeclare -- intentional const/type pattern for enum extension
export type ConnectionSyncStatus =
  | (typeof GeneratedConnectionSyncStatus)[keyof typeof GeneratedConnectionSyncStatus]
  | "queued";

// TODO(https://github.com/airbytehq/hydra-issues-internal/issues/106): Remove when backend adds "queued" to JobStatus
// eslint-disable-next-line @typescript-eslint/no-redeclare -- intentional const/type pattern for enum extension
export const JobStatus = {
  ...GeneratedJobStatus,
  queued: "queued" as const,
};

// eslint-disable-next-line @typescript-eslint/no-redeclare -- intentional const/type pattern for enum extension
export type JobStatus = (typeof GeneratedJobStatus)[keyof typeof GeneratedJobStatus] | "queued";

// TODO(https://github.com/airbytehq/hydra-issues-internal/issues/106): Remove when backend adds queued field to WebBackendConnectionStatusCounts
export interface WebBackendConnectionStatusCounts extends GeneratedWebBackendConnectionStatusCounts {
  queued: number;
}
