import { FormattedMessage } from "react-intl";

import { ConnectionSyncButtons } from "components/connection/ConnectionSync/ConnectionSyncButtons";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";

import { HistoricalOverview } from "area/connection/components";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useExperiment } from "hooks/services/Experiment";

import { ConnectionStatusMessages } from "./ConnectionStatusMessages";
import { ConnectionStatusOverview } from "./ConnectionStatusOverview";

export const ConnectionStatusCard: React.FC = () => {
  const { connection } = useConnectionEditService();
  const {
    syncCatalog: { streams },
  } = connection;
  const streamCount = streams.reduce((count, stream) => count + (stream.config?.selected ? 1 : 0), 0);

  const showHistoricalOverview = useExperiment("connection.streamCentricUI.historicalOverview", false);

  return (
    <Card
      title={
        <FlexContainer justifyContent="space-between" alignItems="center">
          <ConnectionStatusOverview />
          <ConnectionSyncButtons buttonText={<FormattedMessage id="connection.startSync" values={{ streamCount }} />} />
        </FlexContainer>
      }
    >
      <ConnectionStatusMessages />
      {showHistoricalOverview && <HistoricalOverview />}
    </Card>
  );
};
