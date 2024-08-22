import { StreamStatusType } from "components/connection/StreamStatusIndicator";

import {
  AirbyteStream,
  AirbyteStreamAndConfiguration,
  ConnectionSyncResultRead,
  JobConfigType,
  StreamStatusIncompleteRunCause,
  StreamStatusJobType,
  StreamStatusRead,
  StreamStatusRunState,
} from "core/api/types/AirbyteClient";

export type AirbyteStreamAndConfigurationWithEnforcedStream = AirbyteStreamAndConfiguration & { stream: AirbyteStream };

export const activeStatuses: readonly StreamStatusType[] = [
  StreamStatusType.Syncing,
  StreamStatusType.Queued,
  StreamStatusType.Clearing,
  StreamStatusType.Refreshing,
] as const;

export function getStreamKey(streamStatus: StreamStatusRead | AirbyteStream | ConnectionSyncResultRead) {
  let name: string;
  let namespace: string | undefined;

  if ("connectionId" in streamStatus) {
    // StreamStatusRead
    name = streamStatus.streamName;
    namespace = streamStatus.streamNamespace;
  } else if ("streamName" in streamStatus) {
    // ConnectionSyncResultRead
    name = streamStatus.streamName;
    namespace = streamStatus.streamNamespace;
  } else {
    // AirbyteStream
    name = streamStatus.name;
    namespace = streamStatus.namespace;
  }

  if (namespace === undefined) {
    // opting to unify `undefined` into ""
    // instead of the string literal "undefined" which
    // could conflict with a namespace called "undefined"
    namespace = "";
  }

  return `${name}-${namespace}`;
}

interface BaseUIStreamStatus {
  status: Exclude<StreamStatusType, "rateLimited"> | undefined;
  lastSuccessfulSync: StreamStatusRead | undefined;
  isRunning: boolean; // isRunning can be removed once sync progress has been fully rolled out, as it can be inferred from the status.  until then, it is required for the v1 stream status ui.
}
interface RateLimitedUIStreamStatus {
  status: StreamStatusType.RateLimited;
  lastSuccessfulSync: StreamStatusRead | undefined;
  isRunning: true;
  quotaReset?: number;
}

type UIStreamStatus = BaseUIStreamStatus | RateLimitedUIStreamStatus;

export const computeStreamStatus = ({
  statuses,
  hasBreakingSchemaChange,
  isSyncing,
  recordsExtracted,
  runningJobConfigType,
  isRateLimitedUiEnabled = true, // default to `true` here so unit tests execute against this side, this value is set from the one actual call site
}: {
  statuses: StreamStatusRead[];
  hasBreakingSchemaChange: boolean;
  isSyncing: boolean;
  recordsExtracted?: number;
  runningJobConfigType?: string;
  isRateLimitedUiEnabled?: boolean;
}): UIStreamStatus => {
  // no statuses
  if (statuses == null || statuses.length === 0) {
    return {
      status: StreamStatusType.QueuedForNextSync,
      isRunning: false,
      lastSuccessfulSync: undefined,
    };
  }

  const isRunning =
    isSyncing ||
    statuses[0].runState === StreamStatusRunState.RUNNING ||
    statuses[0].runState === StreamStatusRunState.RATE_LIMITED;

  const lastSuccessfulSync = statuses.find(
    ({ jobType, runState }) => jobType === StreamStatusJobType.SYNC && runState === StreamStatusRunState.COMPLETE
  );

  // queued for next sync
  if (
    !isRunning &&
    (statuses[0].runState === StreamStatusRunState.PENDING || statuses[0].jobType === StreamStatusJobType.RESET)
  ) {
    return {
      status: StreamStatusType.QueuedForNextSync,
      isRunning,
      lastSuccessfulSync,
    };
  }

  if (isRateLimitedUiEnabled && statuses[0].runState === StreamStatusRunState.RATE_LIMITED) {
    return {
      status: StreamStatusType.RateLimited,
      isRunning: true,
      lastSuccessfulSync,
      quotaReset: statuses[0].metadata?.quotaReset,
    };
  }

  // queued
  if (isRunning) {
    if (runningJobConfigType === JobConfigType.reset_connection || runningJobConfigType === JobConfigType.clear) {
      return {
        status: StreamStatusType.Clearing,
        isRunning,
        lastSuccessfulSync,
      };
    }

    if (!recordsExtracted || recordsExtracted === 0) {
      return {
        status: StreamStatusType.Queued,
        isRunning,
        lastSuccessfulSync,
      };
    }
    if (recordsExtracted && recordsExtracted > 0) {
      // syncing or refreshing
      if (runningJobConfigType === "sync") {
        return {
          status: StreamStatusType.Syncing,
          isRunning,
          lastSuccessfulSync,
        };
      } else if (runningJobConfigType === "refresh") {
        return {
          status: StreamStatusType.Refreshing,
          isRunning,
          lastSuccessfulSync,
        };
      }
    }
  }

  // breaking schema change means user action is required
  if (hasBreakingSchemaChange) {
    return {
      status: StreamStatusType.Failed,
      isRunning,
      lastSuccessfulSync,
    };
  }

  if (statuses[0].runState === StreamStatusRunState.PENDING) {
    return {
      status: StreamStatusType.Pending,
      isRunning,
      lastSuccessfulSync,
    };
  }

  // reset streams are pending, regardless of the run state
  if (statuses[0].jobType === StreamStatusJobType.RESET) {
    return {
      status: StreamStatusType.Pending,
      isRunning,
      lastSuccessfulSync,
    };
  }

  // is there are error?
  if (
    statuses[0].jobType === StreamStatusJobType.SYNC &&
    statuses[0].runState === StreamStatusRunState.INCOMPLETE &&
    statuses[0].incompleteRunCause === StreamStatusIncompleteRunCause.FAILED
  ) {
    return {
      status: StreamStatusType.Incomplete,
      isRunning,
      lastSuccessfulSync,
    };
  }

  // completed syncs that haven't been flagged as anything else are synced
  if (lastSuccessfulSync) {
    return {
      status: StreamStatusType.Synced,
      isRunning,
      lastSuccessfulSync,
    };
  }

  return {
    status: undefined,
    isRunning,
    lastSuccessfulSync,
  };
};
