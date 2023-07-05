import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { FormControl } from "components/forms";

import { ConnectionSettingsFormValues } from "./ConnectionSettingsPage";

interface SchemaUpdateNotificationsProps {
  disabled?: boolean;
}

export const SchemaUpdateNotifications: React.FC<SchemaUpdateNotificationsProps> = ({ disabled }) => {
  const { formatMessage } = useIntl();

  return (
    <FormControl<ConnectionSettingsFormValues>
      disabled={disabled}
      label={formatMessage({ id: "connection.schemaUpdateNotifications.title" })}
      description={<FormattedMessage id="connection.schemaUpdateNotifications.info" />}
      fieldType="switch"
      name="notifySchemaChanges"
      inline
    />
  );
};
