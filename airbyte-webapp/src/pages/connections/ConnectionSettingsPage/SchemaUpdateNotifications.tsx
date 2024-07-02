import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { FormControl } from "components/forms";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { useCurrentWorkspace } from "core/api";
import { RoutePaths, SettingsRoutePaths } from "pages/routePaths";

import { ConnectionSettingsFormValues } from "./ConnectionSettingsPage";

interface SchemaUpdateNotificationsProps {
  disabled?: boolean;
}

export const SchemaUpdateNotifications: React.FC<SchemaUpdateNotificationsProps> = ({ disabled }) => {
  const { formatMessage } = useIntl();
  const { notificationSettings } = useCurrentWorkspace();
  const hasWorkspaceConnectionUpdateNotifications =
    notificationSettings?.sendOnConnectionUpdate?.notificationType &&
    notificationSettings.sendOnConnectionUpdate.notificationType.length > 0;

  const createLink = useCurrentWorkspaceLink();

  return (
    <FlexContainer>
      <FormControl<ConnectionSettingsFormValues>
        disabled={disabled}
        label={formatMessage({ id: "connection.schemaUpdateNotifications.title" })}
        description={<FormattedMessage id="connection.schemaUpdateNotifications.info" />}
        fieldType="switch"
        name="notifySchemaChanges"
        inline
      />
      {!hasWorkspaceConnectionUpdateNotifications && (
        <>
          {" "}
          <Icon type="infoFilled" color="disabled" />
          <Text color="grey">
            <FormattedMessage
              id="connection.schemaUpdateNotifications.workspaceWarning"
              values={{
                link: (children) => (
                  <Link to={createLink(`/${RoutePaths.Settings}/${SettingsRoutePaths.Notifications}`)}>{children}</Link>
                ),
              }}
            />
          </Text>
        </>
      )}
    </FlexContainer>
  );
};
