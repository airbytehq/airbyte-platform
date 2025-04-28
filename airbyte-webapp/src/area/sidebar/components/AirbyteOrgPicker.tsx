import React from "react";
import { useIntl } from "react-intl";

import AirbyteLogoIcon from "components/illustrations/airbyte-logo-icon.svg?react";
import { Button } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptions } from "components/ui/DropdownMenu";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationInfo, useCurrentWorkspace } from "core/api";

import styles from "./AirbyteOrgPicker.module.scss";
export const AirbyteOrgPicker: React.FC = () => {
  const { formatMessage } = useIntl();
  const workspace = useCurrentWorkspace();
  const organization = useCurrentOrganizationInfo();

  const menuOptions: DropdownMenuOptions = [
    {
      as: "div",
      className: styles.orgPicker__dropdownMenu,
      children: (
        <>
          <Text size="sm" color="darkBlue" className={styles.dropdownMenu__orgName}>
            {organization.organizationName}
          </Text>
          <Button className={styles.dropdownMenu__button} variant="secondary" size="sm" icon="gear" />
        </>
      ),
    },
  ];

  return (
    <FlexContainer className={styles.orgPicker} justifyContent="space-between" alignItems="center" gap="sm">
      <DropdownMenu
        options={menuOptions}
        placement="bottom-start"
        displacement={1}
        style={{ zIndex: 10000, borderRadius: "0 8px 8px 8px" }}
      >
        {({ open }) => (
          <button
            className={styles.orgPicker__dropdownButton}
            aria-expanded={open}
            aria-haspopup="true"
            aria-label={formatMessage(
              { id: "sidebar.workspaceAndOrg" },
              { workspace: workspace.name, organization: organization.organizationName }
            )}
          >
            <AirbyteLogoIcon className={styles.orgPicker__logo} aria-hidden="true" />
            <div className={styles.orgPicker__workspace}>
              <Text size="sm" color="darkBlue" className={styles.orgPicker__workspaceName} title={workspace.name}>
                {workspace.name}
              </Text>
              <Text
                size="sm"
                color="grey400"
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
