import React from "react";
import { FormattedMessage } from "react-intl";

import { EmptyState } from "components/ui/EmptyState";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { Spinner } from "components/ui/Spinner";
import { Text } from "components/ui/Text";

import { RoutePaths } from "pages/routePaths";

import styles from "./WorkspacesPanel.module.scss";
import { Region, WorkspacesStatus } from "../../types";

interface WorkspacesPanelProps {
  region: Region;
  workspacesStatus: WorkspacesStatus;
}

export const WorkspacesPanel: React.FC<WorkspacesPanelProps> = ({ region, workspacesStatus }) => {
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
