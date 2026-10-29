import React from "react";

import { StatusBanner } from "cloud/area/billing/components/StatusBanner";
import { AdpOrganizationBanner } from "cloud/components/AdpOrganizationBanner";
import { AgentsOptInBanner } from "cloud/components/AgentsOptIn";
import { FusionUsageBanner } from "cloud/components/AgentsOptIn/FusionUsageBanner";

import { BannerGroupContext } from "./BannerGroupContext";
import styles from "./CloudBannerGroup.module.scss";
import { BannerGroupToggle } from "./components";
import { useBannerGroupState } from "./useBannerGroupState";

export const CloudBannerGroup: React.FC = () => {
  const { contextValue, hideableCount, isCollapsed, isToggleShown, collapseBannerGroup, expandBannerGroup } =
    useBannerGroupState();

  return (
    <BannerGroupContext.Provider value={contextValue}>
      <div className={styles.cloudBannerGroup} data-testid="cloud-banner-group">
        {/* Banners that can be a warning or an error come first: they never collapse, so they keep the top of the group. */}
        <FusionUsageBanner />
        <StatusBanner />
        {/* Banners that are always info come next, then the toggle. Keep this order when adding banners. */}
        <AgentsOptInBanner />
        <AdpOrganizationBanner />
        {isToggleShown && (
          <BannerGroupToggle
            isCollapsed={isCollapsed}
            hiddenCount={hideableCount}
            onToggle={isCollapsed ? expandBannerGroup : collapseBannerGroup}
          />
        )}
      </div>
    </BannerGroupContext.Provider>
  );
};
