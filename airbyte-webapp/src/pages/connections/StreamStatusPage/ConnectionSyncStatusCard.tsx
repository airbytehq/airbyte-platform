import { useIntl } from "react-intl";

import { Card } from "components/ui/Card";

import { HistoricalOverview } from "area/connection/components";
import { FeatureItem, useFeature } from "core/services/features";
import { useExperiment } from "hooks/services/Experiment";

export const ConnectionSyncStatusCard: React.FC = () => {
  const { formatMessage } = useIntl();
  const showHistoricalOverviewFeature = useFeature(FeatureItem.ConnectionHistoryGraphs);
  const showHistoricalOverviewExperiment = useExperiment("connection.streamCentricUI.historicalOverview", false);
  const showHistoricalOverview = showHistoricalOverviewFeature && showHistoricalOverviewExperiment;

  if (!showHistoricalOverview) {
    return null;
  }

  return (
    <Card noPadding title={formatMessage({ id: "connection.syncStatusCard.title" })}>
      <HistoricalOverview />
    </Card>
  );
};
