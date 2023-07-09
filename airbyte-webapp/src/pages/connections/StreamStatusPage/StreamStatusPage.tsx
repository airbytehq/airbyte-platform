import { ConnectionSyncContextProvider } from "components/connection/ConnectionSync/ConnectionSyncContext";

import { StreamsList } from "./StreamsList";
import { StreamsListContextProvider } from "./StreamsListContext";

export const StreamStatusPage = () => {
  return (
    <ConnectionSyncContextProvider>
      <StreamsListContextProvider>
        <StreamsList />
      </StreamsListContextProvider>
    </ConnectionSyncContextProvider>
  );
};
