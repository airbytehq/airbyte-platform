import { ConnectionSyncContextProvider } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { FlexContainer } from "components/ui/Flex";

import { ConnectionStatusCard } from "./ConnectionStatusCard";
import { StreamsList } from "./StreamsList";
import { StreamsListContextProvider } from "./StreamsListContext";

export const StreamStatusPage = () => {
  return (
    <ConnectionSyncContextProvider>
      <StreamsListContextProvider>
        <FlexContainer direction="column" gap="md">
          <ConnectionStatusCard />
          <StreamsList />
        </FlexContainer>
      </StreamsListContextProvider>
    </ConnectionSyncContextProvider>
  );
};
