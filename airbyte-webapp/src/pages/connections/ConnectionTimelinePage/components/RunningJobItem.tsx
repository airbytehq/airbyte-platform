import React from "react";
import { InferType } from "yup";

import { ClearRunningItem } from "./ClearRunningItem";
import { RefreshRunningItem } from "./RefreshRunningItem";
import { SyncRunningItem } from "./SyncRunningItem";
import { jobRunningSchema } from "../types";

export const RunningJobItem: React.FC<{ jobRunningItem: InferType<typeof jobRunningSchema> }> = React.memo(
  ({ jobRunningItem }) => {
    if (!jobRunningItem || !jobRunningItem.createdAt || !jobRunningItem.summary) {
      return null;
    }

    const getStreams = (types: string[]) =>
      jobRunningItem.summary.streams
        .filter((stream) => types.includes(stream.configType))
        .map((stream) => stream.streamName);

    switch (jobRunningItem.summary.configType) {
      case "sync":
        return <SyncRunningItem startedAt={jobRunningItem.createdAt} jobId={jobRunningItem.summary.jobId} />;
      case "refresh":
        return (
          <RefreshRunningItem
            startedAt={jobRunningItem.createdAt}
            jobId={jobRunningItem.summary.jobId}
            streams={getStreams(["refresh"])}
          />
        );
      case "clear":
      case "reset_connection":
        return (
          <ClearRunningItem
            startedAt={jobRunningItem.createdAt}
            jobId={jobRunningItem.summary.jobId}
            streams={getStreams(["clear", "reset_connection"])}
          />
        );
      default:
        return null;
    }
  }
);
RunningJobItem.displayName = "RunningJobItem";
