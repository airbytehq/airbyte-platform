import classNames from "classnames";
import React, { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Badge } from "components/ui/Badge";
import { EmptyState } from "components/ui/EmptyState";
import { FlexContainer } from "components/ui/Flex";
import { Icon, IconType } from "components/ui/Icon";
import AirbyteLogoIcon from "components/ui/illustrations/airbyte-logo-icon.svg?react";
import { ExternalLink, Link } from "components/ui/Link";
import { Spinner } from "components/ui/Spinner";
import { Text } from "components/ui/Text";

import { links } from "core/utils/links";
import { RoutePaths } from "pages/routePaths";

import { DataplaneHealthCard } from "./DataplaneHealthCard";
import { HEALTH_STATUSES, healthStatusIcon, healthStatusSummaryMessageId } from "./healthStatus";
import styles from "./RegionCard.module.scss";
import { Region } from "./regionsPageData";
import { WorkspacesStatus } from "./useRegionsPageData";

interface RegionCardProps {
  region: Region;
  workspacesStatus: WorkspacesStatus;
}

interface PanelProps {
  region: Region;
  workspacesStatus: WorkspacesStatus;
}

type RegionTab = "dataplanes" | "workspaces";

const REGION_TABS: RegionTab[] = ["dataplanes", "workspaces"];

const TAB_LABEL: Record<RegionTab, { icon: IconType; messageId: string; messageIdWithoutCount: string }> = {
  dataplanes: {
    icon: "database",
    messageId: "nestedRegions.dataPlanesTab",
    messageIdWithoutCount: "settings.organization.regions.tab.dataplanes",
  },
  workspaces: {
    icon: "suitcase",
    messageId: "nestedRegions.workspacesTab",
    messageIdWithoutCount: "settings.organizationSettings.regions.workspaces",
  },
};

const DataplaneStatusCounts: React.FC<{ region: Region }> = ({ region }) => {
  const { formatMessage } = useIntl();
  return (
    <FlexContainer alignItems="center" gap="md" data-testid="region-status-counts">
      {HEALTH_STATUSES.map((status) => {
        const count = region.dataplanes.filter((dataplane) => dataplane.status === status).length;
        if (count === 0) {
          return null;
        }
        const icon = healthStatusIcon[status];
        return (
          <FlexContainer
            key={status}
            alignItems="center"
            gap="xs"
            title={formatMessage({ id: healthStatusSummaryMessageId[status] }, { count })}
          >
            <Icon type={icon.type} color={icon.color} size="sm" />
            <Text size="md">{count}</Text>
          </FlexContainer>
        );
      })}
    </FlexContainer>
  );
};

const DataplanesPanel: React.FC<{ region: Region }> = ({ region }) => {
  if (region.isAirbyteManaged) {
    return (
      <FlexContainer alignItems="center" justifyContent="space-between" className={styles.panel}>
        <FlexContainer alignItems="center" gap="lg">
          <AirbyteLogoIcon className={styles.logo} />
          <Text size="lg">
            <FormattedMessage id="settings.organization.regions.airbyteManaged.message" />
          </Text>
        </FlexContainer>
        <ExternalLink href={links.statusLink} variant="button">
          <FormattedMessage id="settings.organization.regions.airbyteManaged.statusPage" />
        </ExternalLink>
      </FlexContainer>
    );
  }

  if (region.dataplanes.length === 0) {
    return (
      <div className={styles.panel}>
        <EmptyState
          icon="chart"
          text={<FormattedMessage id="nestedRegions.noDataplanes" />}
          description={<FormattedMessage id="settings.organization.regions.noDataplanes.description" />}
        />
      </div>
    );
  }

  return (
    <div>
      {region.dataplanes.map((dataplane) => (
        <DataplaneHealthCard key={dataplane.dataplane_id} dataplane={dataplane} />
      ))}
    </div>
  );
};

const WorkspacesPanel: React.FC<PanelProps> = ({ region, workspacesStatus }) => {
  if (workspacesStatus === "loading") {
    return (
      <FlexContainer justifyContent="center" className={styles.panel} data-testid="workspaces-loading">
        <Spinner size="sm" />
      </FlexContainer>
    );
  }

  if (workspacesStatus === "error") {
    return (
      <div className={styles.panel}>
        <Text color="red" size="md" data-testid="workspaces-error">
          <FormattedMessage id="settings.organization.regions.workspaces.loadFailed" />
        </Text>
      </div>
    );
  }

  if (region.workspaces.length === 0) {
    return (
      <div className={styles.panel}>
        <EmptyState
          icon="chart"
          text={<FormattedMessage id="nestedRegions.noWorkspaces" />}
          description={<FormattedMessage id="settings.organization.regions.noWorkspaces.description" />}
        />
      </div>
    );
  }

  return (
    <div>
      {region.workspaces.map((workspace) => (
        <FlexContainer
          key={workspace.workspaceId}
          alignItems="center"
          justifyContent="space-between"
          className={styles.workspaceRow}
        >
          <FlexContainer alignItems="center" gap="md">
            <Icon type="suitcase" size="sm" color="action" />
            <Text size="md">{workspace.name}</Text>
          </FlexContainer>
          <FlexContainer alignItems="center" gap="sm">
            <Link to={`/${RoutePaths.Workspaces}/${workspace.workspaceId}/${RoutePaths.Connections}`} variant="button">
              <FormattedMessage id="settings.organization.regions.workspace.connections" />
            </Link>
            <Link to={`/${RoutePaths.Workspaces}/${workspace.workspaceId}/${RoutePaths.Settings}`} variant="button">
              <FormattedMessage id="settings.organization.regions.workspace.settings" />
            </Link>
          </FlexContainer>
        </FlexContainer>
      ))}
    </div>
  );
};

const TabLabel: React.FC<{ tab: RegionTab; count?: number }> = ({ tab, count }) => {
  if (count === undefined) {
    return <FormattedMessage id={TAB_LABEL[tab].messageIdWithoutCount} />;
  }
  return <FormattedMessage id={TAB_LABEL[tab].messageId} values={{ count }} />;
};

const PANEL_BY_TAB: Record<RegionTab, React.FC<PanelProps>> = {
  dataplanes: DataplanesPanel,
  workspaces: WorkspacesPanel,
};

interface RegionTabsProps extends PanelProps {
  activeTab: RegionTab;
  onSelectTab: (tab: RegionTab) => void;
}

// The workspaces count is unknown until the query settles, so the tab shows no count before that.
const RegionTabs: React.FC<RegionTabsProps> = ({ region, workspacesStatus, activeTab, onSelectTab }) => {
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
