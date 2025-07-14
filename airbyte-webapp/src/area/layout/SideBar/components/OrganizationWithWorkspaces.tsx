import classNames from "classnames";
import { FormattedMessage, useIntl } from "react-intl";
import { NavLink } from "react-router-dom";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { OrganizationSummary } from "core/api/types/AirbyteClient";
import { CloudSettingsRoutePaths } from "packages/cloud/views/settings/routePaths";
import { RoutePaths } from "pages/routePaths";

import styles from "./OrganizationWithWorkspaces.module.scss";

export const OrganizationWithWorkspaces: React.FC<OrganizationSummary & { lastItem: boolean }> = ({
  organization,
  workspaces = [],
  memberCount,
  subscriptionName,
  lastItem,
}) => {
  const { formatMessage } = useIntl();
  return (
    <FlexContainer direction="column" gap="none">
      <FlexContainer
        direction="row"
        gap="xs"
        alignItems="center"
        justifyContent="space-between"
        className={styles.organizationNameContainer}
      >
        <NavLink
          to={`${RoutePaths.Organization}/${organization.organizationId}/${RoutePaths.Workspaces}`}
          className={styles.organizationName}
        >
          <div className={styles.orgTextBlock}>
            <Text size="sm" color="darkBlue" bold className={styles.orgTitle}>
              {organization.organizationName}
            </Text>
            <Text
              size="sm"
              color="grey400"
              className={styles.orgMeta}
              title={`${formatMessage({ id: "organization.subscription" }, { subscriptionName })} ${formatMessage(
                { id: "organization.members" },
                { count: memberCount || 0 }
              )}`}
            >
              {subscriptionName && <FormattedMessage id="organization.subscription" values={{ subscriptionName }} />}
              <FormattedMessage id="organization.members" values={{ count: memberCount || 0 }} />
            </Text>
          </div>
        </NavLink>
        <NavLink
          to={`${RoutePaths.Organization}/${organization.organizationId}/${RoutePaths.Settings}/${CloudSettingsRoutePaths.Organization}`}
          className={styles.gearIconLink}
        >
          <Icon type="gear" className={styles.gearIcon} aria-hidden="true" />
        </NavLink>
      </FlexContainer>
      <FlexContainer
        direction="column"
        gap="none"
        className={classNames(styles.workspaces, { [styles.lastItem]: lastItem })}
      >
        {workspaces.map((workspace) => (
          <NavLink
            key={workspace.workspaceId}
            to={`${RoutePaths.Workspaces}/${workspace.workspaceId}`}
            className={styles.workspaceItem}
          >
            {workspace.name}
          </NavLink>
        ))}
      </FlexContainer>
    </FlexContainer>
  );
};
