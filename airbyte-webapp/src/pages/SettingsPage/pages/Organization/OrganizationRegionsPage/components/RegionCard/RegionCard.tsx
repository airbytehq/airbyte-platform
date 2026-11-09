import classNames from "classnames";
import React, { useState } from "react";
import { FormattedMessage } from "react-intl";

import { Badge } from "components/ui/Badge";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import styles from "./RegionCard.module.scss";
import { Region, WorkspacesStatus } from "../../types";
import { DataplanesPanel } from "../DataplanesPanel";
import { DataplaneStatusCounts } from "../DataplaneStatusCounts";
import { RegionTab, RegionTabs } from "../RegionTabs";
import { WorkspacesPanel } from "../WorkspacesPanel";

interface RegionCardProps {
  region: Region;
  workspacesStatus: WorkspacesStatus;
}

const PANEL_BY_TAB: Record<RegionTab, React.FC<RegionCardProps>> = {
  dataplanes: DataplanesPanel,
  workspaces: WorkspacesPanel,
};

export const RegionCard: React.FC<RegionCardProps> = ({ region, workspacesStatus }) => {
  const [isExpanded, setIsExpanded] = useState(false);
  const [activeTab, setActiveTab] = useState<RegionTab>("dataplanes");
  const Panel = PANEL_BY_TAB[activeTab];

  return (
    <div className={styles.card} data-testid={`region-card-${region.regionId}`}>
      <button
        type="button"
        className={styles.header}
        onClick={() => setIsExpanded((expanded) => !expanded)}
        aria-expanded={isExpanded}
      >
        <FlexContainer alignItems="center" gap="sm">
          <Text size="lg">{region.name}</Text>
          <Badge variant={region.isAirbyteManaged ? "blue" : "grey"} radius="2xs">
            <FormattedMessage
              id={
                region.isAirbyteManaged
                  ? "settings.organization.regions.badge.airbyteManaged"
                  : "settings.organization.regions.badge.selfManaged"
              }
            />
          </Badge>
        </FlexContainer>
        <FlexContainer alignItems="center" gap="sm">
          {!region.isAirbyteManaged && <DataplaneStatusCounts region={region} />}
          <Icon
            type="chevronRight"
            size="lg"
            color="affordance"
            className={classNames(styles.chevron, { [styles.chevronExpanded]: isExpanded })}
          />
        </FlexContainer>
      </button>

      {isExpanded && (
        <>
          <RegionTabs
            region={region}
            workspacesStatus={workspacesStatus}
            activeTab={activeTab}
            onSelectTab={setActiveTab}
          />
          <Panel region={region} workspacesStatus={workspacesStatus} />
        </>
      )}
    </div>
  );
};
