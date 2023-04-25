import React from "react";
import { useIntl } from "react-intl";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Card } from "components/ui/Card";

import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useNotificationService } from "hooks/services/Notification";

const connectionNameFormSchema = yup.object({
  connectionName: yup.string().required("form.empty.error"),
});

interface ConnectionNameFormValues {
  connectionName: string;
}

export const UpdateConnectionName: React.FC = () => {
  const { connection, updateConnection } = useConnectionEditService();
  const { registerNotification } = useNotificationService();
  const { trackError } = useAppMonitoringService();
  const { formatMessage } = useIntl();

  const onSuccess = () => {
    registerNotification({
      id: "connection_name_change_success",
      text: formatMessage({ id: "form.changesSaved" }),
      type: "success",
    });
  };

  const onError = (e: Error, { connectionName }: ConnectionNameFormValues) => {
    trackError(e, { connectionName });
    registerNotification({
      id: "connection_name_change_error",
      text: formatMessage({ id: "connection.updateFailed" }),
      type: "error",
    });
  };

  return (
    <Card withPadding>
      <Form<ConnectionNameFormValues>
        trackDirtyChanges
        onSubmit={({ connectionName }) =>
          updateConnection({
            name: connectionName,
            connectionId: connection.connectionId,
          })
        }
        onError={onError}
        onSuccess={onSuccess}
        schema={connectionNameFormSchema}
        defaultValues={{ connectionName: connection.name }}
      >
        <FormControl<ConnectionNameFormValues>
          label={formatMessage({ id: "form.connectionName" })}
          fieldType="input"
          name="connectionName"
        />
        <FormSubmissionButtons submitKey="settings.accountSettings.updateName" />
      </Form>
    </Card>
  );
};
