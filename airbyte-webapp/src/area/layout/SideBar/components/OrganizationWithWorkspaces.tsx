import { FormattedMessage } from "react-intl";
import { NavLink } from "react-router-dom";
import { ItemContent } from "react-virtuoso";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { OrganizationSummary } from "core/api/types/AirbyteClient";
import { CloudSettingsRoutePaths } from "packages/cloud/views/settings/routePaths";
import { RoutePaths } from "pages/routePaths";

import styles from "./OrganizationWithWorkspaces.module.scss";

export const OrganizationWithWorkspaces: ItemContent<OrganizationSummary, null> = (
  _index,
  { organization, workspaces = [], memberCount, subscription }
) => {
  return (
    <FlexContainer direction="column" gap="none">
      <NavLink
        to={`${RoutePaths.Organization}/${organization.organizationId}/${RoutePaths.Settings}/${CloudSettingsRoutePaths.Organization}`}
        className={styles.organizationName}
      >
        <div className={styles.orgTextBlock}>
          <Text size="sm" color="darkBlue" bold className={styles.orgTitle}>
            {organization.organizationName}
          </Text>
          <Text size="sm" color="grey400" className={styles.orgMeta}>
            {subscription && <>{subscription.name} &bull; </>}
            <FormattedMessage id="organization.members" values={{ count: memberCount || 0 }} />
          </Text>
        </div>
        <Icon type="gear" className={styles.gearIcon} aria-hidden="true" />
      </NavLink>
      <FlexContainer direction="column" gap="none">
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
