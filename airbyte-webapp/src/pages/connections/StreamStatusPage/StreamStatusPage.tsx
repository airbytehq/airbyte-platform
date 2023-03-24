import { ConnectionSyncContextProvider } from "components/connection/ConnectionSync/ConnectionSyncContext";

import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useListJobs } from "services/job/JobService";

import { StreamsList } from "./StreamsList";
import { StreamsListContextProvider } from "./StreamsListContext";

export const StreamStatusPage = () => {
  const { connection } = useConnectionEditService();
  const { jobs } = useListJobs({
    configId: connection.connectionId,
    configTypes: ["sync", "reset_connection"],
    pagination: {
      pageSize: 10,
    },
  });
  return (
    <StreamsListContextProvider jobs={jobs}>
      <ConnectionSyncContextProvider jobs={jobs}>
        <StreamsList />
      </ConnectionSyncContextProvider>
    </StreamsListContextProvider>
  );
};
