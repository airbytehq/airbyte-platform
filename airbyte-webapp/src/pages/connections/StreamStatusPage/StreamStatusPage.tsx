import { ConnectionSyncContextProvider } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { Box } from "components/ui/Box";

import { ConnectionStatusCard } from "./ConnectionStatusCard";
import { StreamsList } from "./StreamsList";
import { StreamsListContextProvider } from "./StreamsListContext";

export const StreamStatusPage = () => {
  return (
    <ConnectionSyncContextProvider>
      <StreamsListContextProvider>
        <Box mb="md">
          <ConnectionStatusCard />
        </Box>
        <StreamsList />
      </StreamsListContextProvider>
    </ConnectionSyncContextProvider>
  );
};
