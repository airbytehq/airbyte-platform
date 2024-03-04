import { Suspense, useState } from "react";
import { FormattedMessage } from "react-intl";

import { LoadingPage } from "components";
import { Box } from "components/ui/Box";
import { Tabs, ButtonTab } from "components/ui/Tabs";

import { useExperiment } from "hooks/services/Experiment";

import { DataMovedGraph } from "../DataMovedGraph";
import { UptimeStatusGraph } from "../UptimeStatusGraph";

export const HistoricalOverview: React.FC = () => {
  const doUseStreamStatuses = useExperiment("connection.streamCentricUI.v2", false);
  const [selectedTab, setSelectedTab] = useState<"uptimeStatus" | "dataMoved">(
    doUseStreamStatuses ? "uptimeStatus" : "dataMoved"
  );

  return (
    <Box p="lg">
      <Tabs>
        {doUseStreamStatuses && (
          <ButtonTab
            id="uptimeStatus"
            name={<FormattedMessage id="connection.overview.graph.uptimeStatus" />}
            isActive={selectedTab === "uptimeStatus"}
            onSelect={() => setSelectedTab("uptimeStatus")}
          />
        )}
        <ButtonTab
          id="dataMoved"
          name={<FormattedMessage id="connection.overview.graph.dataMoved" />}
          isActive={selectedTab === "dataMoved"}
          onSelect={() => setSelectedTab("dataMoved")}
        />
      </Tabs>
      <Box pt="sm">
        <Suspense fallback={<LoadingPage />}>
          {selectedTab === "uptimeStatus" && <UptimeStatusGraph />}
          {selectedTab === "dataMoved" && <DataMovedGraph />}
        </Suspense>
      </Box>
    </Box>
  );
};
