import classNames from "classnames";
import { FormattedMessage } from "react-intl";
import { NavLink } from "react-router-dom";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { OrganizationSummary } from "core/api/types/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";
import { CloudSettingsRoutePaths } from "packages/cloud/views/settings/routePaths";
import { RoutePaths } from "pages/routePaths";

import styles from "./OrganizationWithWorkspaces.module.scss";

export const OrganizationWithWorkspaces: React.FC<
  Omit<OrganizationSummary, "subscriptionName"> & { brandingBadge?: React.ReactNode; lastItem: boolean }
> = ({ organization, workspaces = [], memberCount, brandingBadge, lastItem }) => {
  const allowMultiWorkspace = useFeature(FeatureItem.MultiWorkspaceUI);
  const WORKSPACES_NAV_LINK = `${RoutePaths.Organization}/${organization.organizationId}/${RoutePaths.Workspaces}`;
  const SETTINGS_NAV_LINK = `${RoutePaths.Organization}/${organization.organizationId}/${RoutePaths.Settings}/${CloudSettingsRoutePaths.Organization}`;
  return (
    <FlexContainer direction="column" gap="none">
      <FlexContainer
        direction="row"
        gap="xs"
        alignItems="center"
        justifyContent="space-between"
        className={styles.organizationNameContainer}
      >
        <NavLink to={allowMultiWorkspace ? WORKSPACES_NAV_LINK : SETTINGS_NAV_LINK} className={styles.organizationName}>
          <div className={styles.orgTextBlock}>
            <Text size="sm" color="darkBlue" bold className={styles.orgTitle}>
              {organization.organizationName}
            </Text>
            <FlexContainer direction="row" gap="sm" alignItems="center">
              {brandingBadge && brandingBadge}
              {allowMultiWorkspace && (
                <Text size="sm" color="grey400" className={styles.orgMeta}>
                  <FormattedMessage id="organization.members" values={{ count: memberCount || 0 }} />
                </Text>
              )}
            </FlexContainer>
          </div>
        </NavLink>
        <NavLink to={SETTINGS_NAV_LINK} className={styles.gearIconLink}>
          <Icon type="gear" className={styles.gearIcon} aria-hidden="true" />
        </NavLink>
      </FlexContainer>
      <FlexContainer
        direction="column"
        gap="none"
        className={classNames(styles.workspaces, { [styles.lastItem]: lastItem })}
      >
        {workspaces.map(({ workspaceId, name }) => (
          <WorkspaceNavLink key={workspaceId} workspaceId={workspaceId} name={name} />
        ))}
      </FlexContainer>
    </FlexContainer>
  );
};

export const WorkspaceNavLink: React.FC<{
  workspaceId: string;
  name: string;
}> = ({ workspaceId, name }) => {
  return (
    <NavLink to={`${RoutePaths.Workspaces}/${workspaceId}`} className={styles.workspaceItem}>
      {name}
    </NavLink>
  );
};
