import { useFloating, autoUpdate, offset } from "@floating-ui/react-dom";
import { Popover, PopoverButton, PopoverPanel } from "@headlessui/react";
import React from "react";
import { useIntl } from "react-intl";

import AirbyteLogoIcon from "components/illustrations/airbyte-logo-icon.svg?react";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useCurrentWorkspaceOrUndefined, useOrganization } from "core/api";
import { useIsCloudApp } from "core/utils/app";

import styles from "./AirbyteOrgPicker.module.scss";
import { AirbyteOrgPopoverPanel } from "./AirbyteOrgPopoverPanel";

export const AirbyteOrgPicker: React.FC = () => {
  const { formatMessage } = useIntl();
  const isCloud = useIsCloudApp();
  const workspace = useCurrentWorkspaceOrUndefined();
  const workspaceName = isCloud ? workspace?.name : workspace && formatMessage({ id: "sidebar.myWorkspace" });

  const currentOrganizationId = useCurrentOrganizationId();
  const organization = useOrganization(currentOrganizationId);

  const { x, y, reference, floating, strategy } = useFloating({
    middleware: [offset({ mainAxis: 1, crossAxis: 5 })],
    whileElementsMounted: autoUpdate,
    placement: "bottom-start",
  });

  return (
    <Popover className={styles.orgPicker}>
      {() => (
        <>
          <PopoverButton ref={reference} className={styles.orgPicker__button}>
            <FlexContainer alignItems="center" gap="sm">
              <AirbyteLogoIcon className={styles.orgPicker__logo} aria-hidden="true" />
              <div className={styles.orgPicker__textBlock}>
                {workspace && (
                  <Text
                    size="sm"
                    color="darkBlue"
                    bold
                    className={styles.orgPicker__workspaceName}
                    title={workspaceName}
                  >
                    {workspaceName}
                  </Text>
                )}
                <Text
                  size="sm"
                  color={workspace ? "grey400" : "darkBlue"}
                  className={styles.orgPicker__orgName}
                  title={organization.organizationName}
                >
                  {organization.organizationName}
                </Text>
              </div>
              <Icon type="chevronUpDown" size="xs" color="disabled" />
            </FlexContainer>
          </PopoverButton>
          <PopoverPanel
            ref={floating}
            style={{
              position: strategy,
              top: y ?? 0,
              left: x ?? 0,
            }}
            className={styles.orgPicker__popoverPanel}
            data-testid="orgPicker__popoverPanel"
          >
            <AirbyteOrgPopoverPanel />
          </PopoverPanel>
        </>
      )}
    </Popover>
  );
};
