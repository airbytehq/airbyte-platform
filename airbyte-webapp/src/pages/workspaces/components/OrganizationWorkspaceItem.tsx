import classNames from "classnames";
import React from "react";

import { Box } from "components/ui/Box";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";

import { useWorkspaceConnectionStatusCounts } from "core/api";

import styles from "./OrganizationWorkspaceItem.module.scss";

interface OrganizationWorkspaceItemProps {
  workspace: {
    workspaceId: string;
    name: string;
  };
  filterUnhealthyWorkspaces?: boolean;
  filterRunningSyncs?: boolean;
}

export const OrganizationWorkspaceItem: React.FC<OrganizationWorkspaceItemProps> = ({
  workspace,
  filterUnhealthyWorkspaces = false,
  filterRunningSyncs = false,
}) => {
  const connectionStatusCounts = useWorkspaceConnectionStatusCounts(workspace.workspaceId);
  const pendingCount = connectionStatusCounts?.pendingCount || 0;
  const successCount = connectionStatusCounts?.successCount || 0;
  const failedCount = connectionStatusCounts?.failedCount || 0;

  // If filtering is active, check if this workspace should be shown
  if (filterUnhealthyWorkspaces || filterRunningSyncs) {
    const hasUnhealthyConnections = failedCount > 0;
    const hasRunningSyncs = pendingCount > 0;

    // If both filters are active, show workspaces that match at least one criteria
    if (filterUnhealthyWorkspaces && filterRunningSyncs) {
      if (!hasUnhealthyConnections && !hasRunningSyncs) {
        return null;
      }
    }
    // If only unhealthy filter is active, show only workspaces with failed connections
    else if (filterUnhealthyWorkspaces && !hasUnhealthyConnections) {
      return null;
    }
    // If only running syncs filter is active, show only workspaces with pending connections
    else if (filterRunningSyncs && !hasRunningSyncs) {
      return null;
    }
  }

  return (
    <Box pb="md" key={workspace.workspaceId}>
      <Link to={`/workspaces/${workspace.workspaceId}`} variant="primary">
        <div className={styles.orgWorkspaceItem}>
          <span className={styles.orgWorkspaceItem__name}>{workspace.name}</span>
          <div className={styles.orgWorkspaceItem__meta}>
            <span
              className={classNames(styles.orgWorkspaceItem__badge, {
                [styles["orgWorkspaceItem__badge--sync"]]: pendingCount > 0,
                [styles["orgWorkspaceItem__badge--disabled"]]: pendingCount === 0,
              })}
            >
              <Icon type="statusInProgress" size="xs" />
              {pendingCount}
            </span>
            <span
              className={classNames(styles.orgWorkspaceItem__badge, {
                [styles["orgWorkspaceItem__badge--success"]]: successCount > 0,
                [styles["orgWorkspaceItem__badge--disabled"]]: successCount === 0,
              })}
            >
              <Icon type="check" size="xs" />
              {successCount}
            </span>
            <span
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
