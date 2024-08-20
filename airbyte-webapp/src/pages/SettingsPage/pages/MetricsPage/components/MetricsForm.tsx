import React from "react";
import { useIntl } from "react-intl";
import { SchemaOf } from "yup";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";

import { useCurrentWorkspace, useUpdateWorkspace } from "core/api";
import { trackError } from "core/utils/datadog";
import { useIntent } from "core/utils/rbac";
import { useNotificationService } from "hooks/services/Notification";

interface MetricsFormValues {
  anonymousDataCollection: boolean;
}

const ValidationSchema: SchemaOf<MetricsFormValues> = yup.object().shape({
  anonymousDataCollection: yup.boolean().required(),
});

export const MetricsForm: React.FC = () => {
  const { formatMessage } = useIntl();
  const { workspaceId, organizationId, anonymousDataCollection } = useCurrentWorkspace();
  const { mutateAsync: updateWorkspace } = useUpdateWorkspace();
  const { registerNotification } = useNotificationService();
  const canUpdateWorkspace = useIntent("UpdateWorkspace", { workspaceId, organizationId });

  const onSubmit = async ({ anonymousDataCollection }: MetricsFormValues) => {
    await updateWorkspace({
      workspaceId,
      anonymousDataCollection,
    });
  };

  const onSuccess = () => {
    registerNotification({
      id: "workspace_settings_update_success",
      text: formatMessage({ id: "settings.workspaceSettings.update.success" }),
      type: "success",
    });
  };

  const onError = (e: Error, { anonymousDataCollection }: MetricsFormValues) => {
    trackError(e, { anonymousDataCollection });

    registerNotification({
      id: "workspace_settings_update_error",
      text: formatMessage({ id: "settings.workspaceSettings.update.error" }),
      type: "error",
    });
  };

  return (
    <Form<MetricsFormValues>
      defaultValues={{
        anonymousDataCollection,
      }}
      schema={ValidationSchema}
      onSubmit={onSubmit}
      onSuccess={onSuccess}
      onError={onError}
      disabled={!canUpdateWorkspace}
    >
      <FormControl<MetricsFormValues>
        name="anonymousDataCollection"
        fieldType="switch"
        inline
        label={formatMessage({ id: "preferences.anonymizeUsage" })}
        description={formatMessage({ id: "preferences.collectData" })}
      />

      <FormSubmissionButtons noCancel justify="flex-start" submitKey="form.saveChanges" />
    </Form>
  );
};
