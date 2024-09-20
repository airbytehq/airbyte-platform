import { useEffectOnce } from "react-use";

import { ConnectionSyncContextProvider } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { ScrollParent } from "components/ui/ScrollParent";

import { trackTiming } from "core/utils/datadog";

import { ConnectionStatusMessages } from "./ConnectionStatusMessages";
import { ConnectionSyncStatusCard } from "./ConnectionSyncStatusCard";
import { StreamsList } from "./StreamsList";
import { StreamsListContextProvider } from "./StreamsListContext";
import styles from "./StreamStatusPage.module.scss";

export const StreamStatusPage = () => {
  useEffectOnce(() => {
    trackTiming("StreamStatusPage");
  });

  return (
    <ConnectionSyncContextProvider>
      <StreamsListContextProvider>
        <ScrollParent props={{ className: styles.container }}>
          <ConnectionStatusMessages />
          <ConnectionSyncStatusCard />
          <StreamsList />
        </ScrollParent>
      </StreamsListContextProvider>
    </ConnectionSyncContextProvider>
  );
};
