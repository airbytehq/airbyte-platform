import { useMemo } from "react";

import { getStreamFieldPath, StreamId } from "./types";
import { useBuilderWatch } from "./useBuilderWatch";

export const useStreamNames = () => {
  const streams = useBuilderWatch("manifest.streams");
  const dynamicStreams = useBuilderWatch("manifest.dynamic_streams");
  const streamNames = useMemo(() => streams?.map((stream) => stream.name ?? "") ?? [], [streams]);
  const dynamicStreamNames = useMemo(() => dynamicStreams?.map((stream) => stream.name ?? "") ?? [], [dynamicStreams]);

  return { streamNames, dynamicStreamNames };
};

export const useStreamName = (streamId: StreamId) => {
  const streamNamePath = getStreamFieldPath(streamId, "name");
  return useBuilderWatch(streamNamePath) as string | undefined;
};
