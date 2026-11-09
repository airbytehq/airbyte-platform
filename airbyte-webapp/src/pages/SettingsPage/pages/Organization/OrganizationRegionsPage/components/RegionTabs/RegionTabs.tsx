import classNames from "classnames";
import React from "react";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";

import styles from "./RegionTabs.module.scss";
import { REGION_TABS, TAB_LABEL, RegionTab } from "./regionTabsConfig";
import { TabLabel } from "./TabLabel";
import { Region, WorkspacesStatus } from "../../types";

interface RegionTabsProps {
  region: Region;
  workspacesStatus: WorkspacesStatus;
  activeTab: RegionTab;
  onSelectTab: (tab: RegionTab) => void;
}

// The workspaces count is unknown until the query settles, so the tab shows no count before that.
export const RegionTabs: React.FC<RegionTabsProps> = ({ region, workspacesStatus, activeTab, onSelectTab }) => {
  const countByTab: Record<RegionTab, number | undefined> = {
    dataplanes: region.dataplanes.length,
    workspaces: workspacesStatus === "ready" ? region.workspaces.length : undefined,
  };

  return (
    <FlexContainer className={styles.tabs} gap="none">
      {REGION_TABS.map((tab) => {
        const isActive = tab === activeTab;
        return (
          <button
            key={tab}
            type="button"
            aria-pressed={isActive}
            className={classNames(styles.tab, { [styles.tabActive]: isActive })}
            onClick={() => onSelectTab(tab)}
          >
            <Icon type={TAB_LABEL[tab].icon} size="sm" />
            <TabLabel tab={tab} count={countByTab[tab]} />
          </button>
        );
      })}
    </FlexContainer>
  );
};
