import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { FormControl } from "components/forms";

import { ConnectionSettingsFormValues } from "./ConnectionSettingsPage";

export const SchemaUpdateNotifications: React.FC = () => {
  const { formatMessage } = useIntl();

  return (
    <FormControl<ConnectionSettingsFormValues>
      label={formatMessage({ id: "connection.schemaUpdateNotifications.title" })}
      description={<FormattedMessage id="connection.schemaUpdateNotifications.info" />}
      fieldType="switch"
      name="notifySchemaChanges"
      inline
    />
  );
};
