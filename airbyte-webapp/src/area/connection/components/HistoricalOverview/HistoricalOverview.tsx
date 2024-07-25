import { Suspense, useState } from "react";
import { FormattedMessage } from "react-intl";

import { LoadingPage } from "components";
import { Box } from "components/ui/Box";
import { Tabs, ButtonTab } from "components/ui/Tabs";

import { DefaultErrorBoundary } from "core/errors";

import { DataMovedGraph } from "../DataMovedGraph";
import { UptimeStatusGraph } from "../UptimeStatusGraph";

export const HistoricalOverview: React.FC = () => {
  const [selectedTab, setSelectedTab] = useState<"uptimeStatus" | "dataMoved">("uptimeStatus");

  return (
    <Box p="lg">
      <Tabs>
        <ButtonTab
          id="uptimeStatus"
          name={<FormattedMessage id="connection.overview.graph.uptimeStatus" />}
          isActive={selectedTab === "uptimeStatus"}
          onSelect={() => setSelectedTab("uptimeStatus")}
        />
        <ButtonTab
          id="dataMoved"
          name={<FormattedMessage id="connection.overview.graph.dataMoved" />}
          isActive={selectedTab === "dataMoved"}
          onSelect={() => setSelectedTab("dataMoved")}
        />
      </Tabs>
      <Box pt="sm">
        <DefaultErrorBoundary>
          <Suspense fallback={<LoadingPage />}>
            {selectedTab === "uptimeStatus" && <UptimeStatusGraph />}
            {selectedTab === "dataMoved" && <DataMovedGraph />}
          </Suspense>
        </DefaultErrorBoundary>
      </Box>
    </Box>
  );
};
