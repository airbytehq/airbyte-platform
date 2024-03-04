import { StreamStatusJobType, StreamStatusRead, StreamStatusRunState } from "core/api/types/AirbyteClient";

export const mockStreamStatusRead: StreamStatusRead = {
  id: "123",
  connectionId: "123",
  workspaceId: "123",
  streamName: "foo",
  streamNamespace: "bar",
  jobId: 0,
  attemptNumber: 0,
  jobType: StreamStatusJobType.SYNC,
  runState: StreamStatusRunState.COMPLETE,
  transitionedAt: 0,
};
