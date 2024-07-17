import { useRef } from "react";

import { ConnectionSyncContextProvider } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { ScrollableContainer } from "components/ScrollableContainer";

import { ConnectionStatusMessages } from "./ConnectionStatusMessages";
import { ConnectionSyncStatusCard } from "./ConnectionSyncStatusCard";
import { StreamsList } from "./StreamsList";
import { StreamsListContextProvider } from "./StreamsListContext";
import styles from "./StreamStatusPage.module.scss";

export const StreamStatusPage = () => {
  const ref = useRef<HTMLDivElement>(null);

  return (
    <ConnectionSyncContextProvider>
      <StreamsListContextProvider>
        <ScrollableContainer ref={ref} className={styles.container}>
          <ConnectionStatusMessages />
          <ConnectionSyncStatusCard />
          <StreamsList ref={ref} />
        </ScrollableContainer>
      </StreamsListContextProvider>
    </ConnectionSyncContextProvider>
  );
};
