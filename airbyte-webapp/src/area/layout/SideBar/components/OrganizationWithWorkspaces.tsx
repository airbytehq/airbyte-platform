import { useClose } from "@headlessui/react";
import capitalize from "lodash/capitalize";
import React from "react";
import { useIntl } from "react-intl";
import { NavLink } from "react-router-dom";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useOrganizationUserCount } from "core/api";
import { OrganizationRead, WorkspaceRead } from "core/api/types/AirbyteClient";
import { useWebappConfig } from "core/config";
import { FeatureItem, useFeature } from "core/services/features";
import { RoutePaths } from "pages/routePaths";

import styles from "./OrganizationWithWorkspaces.module.scss";

interface OrganizationWithWorkspacesProps {
  organization: OrganizationRead;
  workspaces: WorkspaceRead[];
}

export const OrganizationWithWorkspaces: React.FC<OrganizationWithWorkspacesProps> = ({ organization, workspaces }) => {
  const { formatMessage } = useIntl();
  const { edition } = useWebappConfig();
  const showOSSWorkspaceName = useFeature(FeatureItem.ShowOSSWorkspaceName);
  const ossWorkspaceName = showOSSWorkspaceName ? formatMessage({ id: "sidebar.myWorkspace" }) : undefined;
  const userCount = useOrganizationUserCount(organization.organizationId);
  const closePopover = useClose();

  return (
    <FlexContainer direction="column" gap="none">
      <NavLink
        to={`${RoutePaths.Organization}/${organization.organizationId}/settings`}
        className={styles.organizationName}
        onClick={closePopover}
      >
        <div className={styles.orgTextBlock}>
          <Text size="sm" color="darkBlue" bold className={styles.orgTitle}>
            {organization.organizationName}
          </Text>
          {!showOSSWorkspaceName && (
            <Text size="sm" color="grey400" className={styles.orgMeta}>
              {capitalize(edition)} &bull; {formatMessage({ id: "organization.members" }, { count: userCount })}
            </Text>
          )}
        </div>
        <Icon type="gear" className={styles.gearIcon} aria-hidden="true" />
      </NavLink>
      <FlexContainer direction="column" gap="none">
        {workspaces.map((workspace) => (
          <NavLink
            key={workspace.workspaceId}
            to={`${RoutePaths.Workspaces}/${workspace.workspaceId}`}
            className={styles.workspaceItem}
            onClick={closePopover}
          >
            {ossWorkspaceName ?? workspace.name}
          </NavLink>
        ))}
      </FlexContainer>
    </FlexContainer>
  );
};
