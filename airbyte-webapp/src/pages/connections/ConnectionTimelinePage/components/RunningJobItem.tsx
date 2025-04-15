import React from "react";
import { z } from "zod";

import { ClearRunningItem } from "./ClearRunningItem";
import { RefreshRunningItem } from "./RefreshRunningItem";
import { SyncRunningItem } from "./SyncRunningItem";
import { jobRunningSchema } from "../types";

interface RunningJobItemProps {
  event: z.infer<typeof jobRunningSchema>;
}

export const RunningJobItem: React.FC<RunningJobItemProps> = React.memo(({ event }) => {
  if (!event || !event.createdAt || !event.summary) {
    return null;
  }

  const getStreams = (types: string[]) =>
    event.summary.streams.filter((stream) => types.includes(stream.configType)).map((stream) => stream.streamName);

  switch (event.summary.configType) {
    case "sync":
      return <SyncRunningItem startedAt={event.createdAt} jobId={event.summary.jobId} />;
    case "refresh":
      return (
        <RefreshRunningItem startedAt={event.createdAt} jobId={event.summary.jobId} streams={getStreams(["refresh"])} />
      );
    case "clear":
    case "reset_connection":
      return (
        <ClearRunningItem
          startedAt={event.createdAt}
          jobId={event.summary.jobId}
          streams={getStreams(["clear", "reset_connection"])}
        />
      );
    default:
      return null;
  }
});
RunningJobItem.displayName = "RunningJobItem";
