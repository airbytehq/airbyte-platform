import React from "react";
import { useIntl } from "react-intl";
import { z } from "zod";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";

import { useCurrentWorkspace, useUpdateWorkspace } from "core/api";
import { trackError } from "core/utils/datadog";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useNotificationService } from "hooks/services/Notification";

const ValidationSchema = z.object({
  anonymousDataCollection: z.boolean(),
});

type MetricsFormValues = z.infer<typeof ValidationSchema>;

export const MetricsForm: React.FC = () => {
  const { formatMessage } = useIntl();
  const { workspaceId, anonymousDataCollection } = useCurrentWorkspace();
  const { mutateAsync: updateWorkspace } = useUpdateWorkspace();
  const { registerNotification } = useNotificationService();
  const canUpdateWorkspace = useGeneratedIntent(Intent.UpdateWorkspace);

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
      zodSchema={ValidationSchema}
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
