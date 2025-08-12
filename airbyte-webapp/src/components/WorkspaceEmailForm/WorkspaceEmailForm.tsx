import { useIntl } from "react-intl";
import { z } from "zod";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";

import { useCurrentWorkspace, useInvalidateWorkspace, useUpdateWorkspace } from "core/api";
import { trackError } from "core/utils/datadog";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useNotificationService } from "hooks/services/Notification";

const ValidationSchema = z.object({
  email: z.string().email("form.email.error"),
});

type WorkspaceEmailFormValues = z.infer<typeof ValidationSchema>;

export const WorkspaceEmailForm = () => {
  const { formatMessage } = useIntl();
  const { mutateAsync: updateWorkspace } = useUpdateWorkspace();
  const { registerNotification } = useNotificationService();
  const { workspaceId, name, email } = useCurrentWorkspace();
  const invalidateWorkspace = useInvalidateWorkspace(workspaceId);
  const canUpdateWorkspace = useGeneratedIntent(Intent.UpdateWorkspace);

  const onSubmit = async ({ email }: WorkspaceEmailFormValues) => {
    await updateWorkspace({
      workspaceId,
      email,
    });

    await invalidateWorkspace();
  };

  const onSuccess = () => {
    registerNotification({
      id: "workspace_settings_update_success",
      text: formatMessage({ id: "settings.workspaceSettings.update.success" }),
      type: "success",
    });
  };

  const onError = (e: Error, { email }: WorkspaceEmailFormValues) => {
    trackError(e, { name, email });

    registerNotification({
      id: "workspace_settings_update_error",
      text: formatMessage({ id: "settings.workspaceSettings.update.error" }),
      type: "error",
    });
  };

  return (
    <Form<WorkspaceEmailFormValues>
      defaultValues={{
        email,
      }}
      zodSchema={ValidationSchema}
      onSubmit={onSubmit}
      onSuccess={onSuccess}
      onError={onError}
      disabled={!canUpdateWorkspace}
    >
      <FormControl<WorkspaceEmailFormValues>
        name="email"
        fieldType="input"
        labelTooltip={formatMessage({ id: "settings.notifications.emailRecipient" })}
        label={formatMessage({ id: "settings.workspaceSettings.updateWorkspaceNameForm.email.label" })}
        placeholder={formatMessage({
          id: "settings.workspaceSettings.updateWorkspaceNameForm.email.placeholder",
        })}
      />
      <FormSubmissionButtons noCancel justify="flex-start" submitKey="form.saveChanges" />
    </Form>
  );
};
