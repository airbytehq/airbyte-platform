import React from "react";
import { useIntl } from "react-intl";

import AirbyteLogoIcon from "components/illustrations/airbyte-logo-icon.svg?react";
import { DropdownMenu, DropdownMenuOptions } from "components/ui/DropdownMenu";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useCurrentOrganization, useCurrentWorkspaceOrUndefined } from "core/api";
import { useIsCloudApp } from "core/utils/app";
import { RoutePaths } from "pages/routePaths";

import styles from "./AirbyteOrgPicker.module.scss";

export const AirbyteOrgPicker: React.FC = () => {
  const { formatMessage } = useIntl();
  const workspace = useCurrentWorkspaceOrUndefined();
  const organization = useCurrentOrganization();
  const isCloudApp = useIsCloudApp();
  const workspaceName = isCloudApp ? workspace?.name : workspace && formatMessage({ id: "sidebar.myWorkspace" });

  const menuOptions: DropdownMenuOptions = [
    {
      as: "a",
      className: styles.orgPicker__dropdownMenuItem,
      icon: <Icon type="gear" className={styles.orgPicker__gearIcon} aria-hidden="true" />,
      iconPosition: "right",
      displayName: organization.organizationName,
      href: `${RoutePaths.Organization}/${organization.organizationId}/${RoutePaths.Workspaces}`,
      internal: true,
    },
  ];

  return (
    <FlexContainer className={styles.orgPicker} justifyContent="space-between" alignItems="center" gap="sm">
      <DropdownMenu
        options={menuOptions}
        placement="bottom-start"
        displacement={1}
        textSize="sm"
        style={{ zIndex: 10000, borderRadius: "0 8px 8px 8px" }}
      >
        {({ open }) => (
          <button
            className={styles.orgPicker__dropdownButton}
            aria-expanded={open}
            aria-haspopup="true"
            aria-label={formatMessage(
              { id: "sidebar.workspaceAndOrg" },
              { workspace: workspaceName, organization: organization.organizationName }
            )}
          >
            <AirbyteLogoIcon className={styles.orgPicker__logo} aria-hidden="true" />
            <div className={styles.orgPicker__workspace}>
              <Text size="sm" color="darkBlue" className={styles.orgPicker__workspaceName} title={workspaceName}>
                {workspaceName}
              </Text>
              <Text
                size="sm"
                color={workspace ? "grey400" : "darkBlue"}
                className={styles.orgPicker__orgName}
                title={organization.organizationName}
              >
                {organization.organizationName}
              </Text>
            </div>
            <Icon type="chevronUpDown" className={styles.orgPicker__chevron} aria-hidden="true" />
          </button>
        )}
      </DropdownMenu>
    </FlexContainer>
  );
};
