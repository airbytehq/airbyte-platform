import classNames from "classnames";
import React from "react";
import { useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";

import { WebBackendConnectionStatusCounts } from "core/api/types/AirbyteClient";

import styles from "./OrganizationWorkspaceItem.module.scss";

interface OrganizationWorkspaceItemProps {
  workspace: {
    workspaceId: string;
    name: string;
    statusCounts?: WebBackendConnectionStatusCounts;
  };
}

export const OrganizationWorkspaceItem: React.FC<OrganizationWorkspaceItemProps> = ({ workspace }) => {
  const { formatMessage } = useIntl();
  const runningCount = workspace.statusCounts?.running || 0;
  const healthyCount = workspace.statusCounts?.healthy || 0;
  const failedCount = workspace.statusCounts?.failed || 0;

  return (
    <Box pb="md" key={workspace.workspaceId}>
      <Link to={`/workspaces/${workspace.workspaceId}`} variant="primary">
        <div className={styles.orgWorkspaceItem}>
          <span className={styles.orgWorkspaceItem__name}>{workspace.name}</span>
          <div className={styles.orgWorkspaceItem__meta}>
            <span
              title={formatMessage({ id: "workspaces.status.runningSyncs" })}
              className={classNames(styles.orgWorkspaceItem__badge, {
                [styles["orgWorkspaceItem__badge--sync"]]: runningCount > 0,
                [styles["orgWorkspaceItem__badge--disabled"]]: runningCount === 0,
              })}
            >
              <Icon type="statusInProgress" size="xs" />
              {runningCount}
            </span>
            <span
              title={formatMessage({ id: "workspaces.status.successfulSyncs" })}
              className={classNames(styles.orgWorkspaceItem__badge, {
                [styles["orgWorkspaceItem__badge--success"]]: healthyCount > 0,
                [styles["orgWorkspaceItem__badge--disabled"]]: healthyCount === 0,
              })}
            >
              <Icon type="check" size="xs" />
              {healthyCount}
            </span>
            <span
              title={formatMessage({ id: "workspaces.status.failedSyncs" })}
              className={classNames(styles.orgWorkspaceItem__badge, {
                [styles["orgWorkspaceItem__badge--error"]]: failedCount > 0,
                [styles["orgWorkspaceItem__badge--disabled"]]: failedCount === 0,
              })}
            >
              <Icon type="cross" size="xs" />
              {failedCount}
            </span>
            <Icon type="chevronRight" size="lg" color="action" className={styles.orgWorkspaceItem__chevron} />
          </div>
        </div>
      </Link>
    </Box>
  );
};

export default OrganizationWorkspaceItem;
