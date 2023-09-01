import { useState } from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Tabs } from "components/ui/Tabs";
import { ButtonTab } from "components/ui/Tabs/ButtonTab";

import { DataMovedGraph } from "../data-moved-graph";

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
        {selectedTab === "uptimeStatus" && <div>uptime</div>}
        {selectedTab === "dataMoved" && <DataMovedGraph />}
      </Box>
    </Box>
  );
};
