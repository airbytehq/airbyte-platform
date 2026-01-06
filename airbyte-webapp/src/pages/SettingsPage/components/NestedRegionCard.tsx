import classNames from "classnames";
import { useState } from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { DataplaneRow } from "./DataplaneRow";
import styles from "./NestedRegionCard.module.scss";
import { DataplaneWithHealth, WorkspaceInfo } from "./useNestedRegionsData";
import { WorkspaceRow } from "./WorkspaceRow";

interface NestedRegionCardProps {
  regionName: string;
  dataplanes: DataplaneWithHealth[];
  workspaces: WorkspaceInfo[];
}

type TabType = "dataplanes" | "workspaces";

export const NestedRegionCard: React.FC<NestedRegionCardProps> = ({ regionName, dataplanes, workspaces }) => {
  const [activeTab, setActiveTab] = useState<TabType>("dataplanes");

  return (
    <Box className={styles.regionCard}>
      <FlexContainer direction="column" gap="none">
        {/* Region header */}
        <FlexContainer className={styles.regionHeader} alignItems="center" justifyContent="space-between">
          <Text size="md">{regionName}</Text>
        </FlexContainer>

        {/* Tab navigation */}
        <FlexContainer className={styles.tabContainer}>
          <button
            className={classNames(styles.tab, { [styles.tabActive]: activeTab === "dataplanes" })}
            onClick={() => setActiveTab("dataplanes")}
          >
            <Icon type="database" size="sm" className={styles.tabIcon} />
            <FormattedMessage id="nestedRegions.dataPlanesTab" values={{ count: dataplanes.length }} />
          </button>
          <button
            className={classNames(styles.tab, { [styles.tabActive]: activeTab === "workspaces" })}
            onClick={() => setActiveTab("workspaces")}
          >
            <Icon type="suitcase" size="sm" className={styles.tabIcon} />
            <FormattedMessage id="nestedRegions.workspacesTab" values={{ count: workspaces.length }} />
          </button>
        </FlexContainer>

        {/* Tab content */}
        <Box className={styles.tabContent}>
          {activeTab === "dataplanes" ? (
            dataplanes.length > 0 ? (
              dataplanes.map((dataplane) => (
                <DataplaneRow
                  key={dataplane.dataplane_id}
                  dataplaneName={dataplane.dataplane_name}
                  status={dataplane.status}
                  lastHeartbeatTimestamp={dataplane.last_heartbeat_timestamp}
                  recentHeartbeats={dataplane.recent_heartbeats}
                  dataplaneVersion={dataplane.dataplane_version}
                />
              ))
            ) : (
              <FlexContainer className={styles.emptyRow} alignItems="center">
                <Text size="sm" color="grey">
                  <FormattedMessage id="nestedRegions.noDataplanes" />
                </Text>
              </FlexContainer>
            )
          ) : workspaces.length > 0 ? (
            workspaces.map((workspace) => (
              <WorkspaceRow key={workspace.workspace_id} workspaceName={workspace.workspace_name} />
            ))
          ) : (
            <FlexContainer className={styles.emptyRow} alignItems="center">
              <Text size="sm" color="grey">
                <FormattedMessage id="nestedRegions.noWorkspaces" />
              </Text>
            </FlexContainer>
          )}
        </Box>
      </FlexContainer>
    </Box>
  );
};
