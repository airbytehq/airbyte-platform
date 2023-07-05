import { useIntl } from "react-intl";

import { FormControl } from "components/forms";

import { ConnectionSettingsFormValues } from "./ConnectionSettingsPage";

export const UpdateConnectionName = () => {
  const { formatMessage } = useIntl();

  return (
    <FormControl<ConnectionSettingsFormValues>
      label={formatMessage({ id: "form.connectionName" })}
      fieldType="input"
      name="connectionName"
    />
  );
};
