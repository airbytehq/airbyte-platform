import { ConnectionSyncContextProvider } from "components/connection/ConnectionSync/ConnectionSyncContext";

import { useListJobs } from "core/api";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useExperiment } from "hooks/services/Experiment";

import { StreamsList } from "./StreamsList";
import { StreamsListContextProvider } from "./StreamsListContext";

export const StreamStatusPage = () => {
  const { connection } = useConnectionEditService();
  const { jobs } = useListJobs({
    configId: connection.connectionId,
    configTypes: ["sync", "reset_connection"],
    pagination: {
      pageSize: useExperiment("connection.streamCentricUI.numberOfLogsToLoad", 10),
    },
  });
  return (
    <ConnectionSyncContextProvider jobs={jobs}>
      <StreamsListContextProvider jobs={jobs}>
        <StreamsList />
      </StreamsListContextProvider>
    </ConnectionSyncContextProvider>
  );
};
