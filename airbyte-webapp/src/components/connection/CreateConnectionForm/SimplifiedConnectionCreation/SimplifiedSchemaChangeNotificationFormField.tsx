import uniqueId from "lodash/uniqueId";
import React, { useState } from "react";
import { Controller, useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { FormConnectionFormValues } from "components/connection/ConnectionForm/formConfig";
import { FormFieldLayout } from "components/connection/ConnectionForm/FormFieldLayout";
import { ControlLabels } from "components/LabeledControl";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";

import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { useCurrentWorkspace } from "core/api";
import { RoutePaths, SettingsRoutePaths } from "pages/routePaths";

export const SimplifiedSchemaChangeNotificationFormField: React.FC<{ disabled?: boolean }> = ({ disabled }) => {
  const { control } = useFormContext<FormConnectionFormValues>();
  const [controlId] = useState(`input-control-${uniqueId()}`);
  const { notificationSettings } = useCurrentWorkspace();

  const hasWorkspaceConnectionUpdateNotifications =
    notificationSettings?.sendOnConnectionUpdate?.notificationType &&
    notificationSettings.sendOnConnectionUpdate.notificationType.length > 0;

  const createLink = useCurrentWorkspaceLink();

  return (
    <Controller
      name="notifySchemaChanges"
      control={control}
      render={({ field }) => (
        <FormFieldLayout alignItems="flex-start" nextSizing>
          <ControlLabels
            htmlFor={controlId}
            label={
              <FlexContainer direction="column" gap="sm">
                <Text bold>
                  <FormattedMessage id="connection.schemaUpdateNotifications.title" />
                </Text>
                <Text size="sm" color="grey">
                  <FormattedMessage id="connection.schemaUpdateNotifications.subtitle" />
                </Text>
              </FlexContainer>
            }
          />
          <FlexContainer direction="row" alignItems="center">
            <Switch id={controlId} checked={field.value} onChange={field.onChange} size="lg" disabled={disabled} />
            {!hasWorkspaceConnectionUpdateNotifications && (
              <FlexContainer direction="row" alignItems="center" gap="xs">
                <Icon type="infoOutline" color="disabled" size="sm" />
                <Text size="sm" color="grey" as="span">
                  <FormattedMessage
                    id="connection.schemaUpdateNotifications.workspaceWarning"
                    values={{
                      link: (children) => (
                        <Link to={createLink(`/${RoutePaths.Settings}/${SettingsRoutePaths.Notifications}`)}>
                          {children}
                        </Link>
                      ),
                    }}
                  />
                </Text>
              </FlexContainer>
            )}
          </FlexContainer>
        </FormFieldLayout>
      )}
    />
  );
};
