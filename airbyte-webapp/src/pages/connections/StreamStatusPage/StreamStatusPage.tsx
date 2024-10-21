import { useEffectOnce } from "react-use";

import { ConnectionSyncContextProvider } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { ScrollParent } from "components/ui/ScrollParent";

import { HistoricalOverview } from "area/connection/components";
import { FeatureItem, useFeature } from "core/services/features";
import { trackTiming } from "core/utils/datadog";

import { ConnectionStatusMessages } from "./ConnectionStatusMessages";
import { StreamsList } from "./StreamsList";
import { StreamsListContextProvider } from "./StreamsListContext";
import styles from "./StreamStatusPage.module.scss";

export const StreamStatusPage = () => {
  const showHistoricalOverviewFeature = useFeature(FeatureItem.ConnectionHistoryGraphs);

  useEffectOnce(() => {
    trackTiming("StreamStatusPage");
  });

  return (
    <ConnectionSyncContextProvider>
      <StreamsListContextProvider>
        <ScrollParent props={{ className: styles.container }}>
          <ConnectionStatusMessages />
          {showHistoricalOverviewFeature && <HistoricalOverview />}
          <StreamsList />
        </ScrollParent>
      </StreamsListContextProvider>
    </ConnectionSyncContextProvider>
  );
};
