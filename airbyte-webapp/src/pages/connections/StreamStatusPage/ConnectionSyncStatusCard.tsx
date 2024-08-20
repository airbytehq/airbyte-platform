import { Card } from "components/ui/Card";

import { HistoricalOverview } from "area/connection/components";
import { FeatureItem, useFeature } from "core/services/features";

export const ConnectionSyncStatusCard: React.FC = () => {
  const showHistoricalOverviewFeature = useFeature(FeatureItem.ConnectionHistoryGraphs);
  const showHistoricalOverview = showHistoricalOverviewFeature;

  if (!showHistoricalOverview) {
    return null;
  }

  return (
    <Card noPadding>
      <HistoricalOverview />
    </Card>
  );
};
