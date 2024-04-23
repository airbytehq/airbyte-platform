import { FormattedMessage } from "react-intl";

import { ConnectionSyncButtons } from "components/connection/ConnectionSync/ConnectionSyncButtons";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";

import { HistoricalOverview } from "area/connection/components";
import { FeatureItem, useFeature } from "core/services/features";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useExperiment } from "hooks/services/Experiment";

import styles from "./ConnectionStatusCard.module.scss";
import { ConnectionStatusMessages } from "./ConnectionStatusMessages";
import { ConnectionStatusOverview } from "./ConnectionStatusOverview";

export const ConnectionStatusCard: React.FC = () => {
  const { connection } = useConnectionEditService();
  const {
    syncCatalog: { streams },
  } = connection;
  const streamCount = streams.reduce((count, stream) => count + (stream.config?.selected ? 1 : 0), 0);

  const showHistoricalOverviewFeature = useFeature(FeatureItem.ConnectionHistoryGraphs);
  const showHistoricalOverviewExperiment = useExperiment("connection.streamCentricUI.historicalOverview", false);
  const showHistoricalOverview = showHistoricalOverviewFeature && showHistoricalOverviewExperiment;

  return (
    <Card noPadding>
      <Box p="xl" className={styles.header}>
        <FlexContainer justifyContent="space-between" alignItems="center">
          <ConnectionStatusOverview />
          <ConnectionSyncButtons buttonText={<FormattedMessage id="connection.startSync" values={{ streamCount }} />} />
        </FlexContainer>
      </Box>
      <Box p="lg">
        <ConnectionStatusMessages />
      </Box>
      {showHistoricalOverview && <HistoricalOverview />}
    </Card>
  );
};
