import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useToggle } from "react-use";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";

import { useCreateCloudWorkspace, useListCloudWorkspaces } from "core/api/cloud";
import { trackError } from "core/utils/datadog";
import { useNotificationService } from "hooks/services/Notification";

interface CreateWorkspaceFormValues {
  name: string;
}

const CreateWorkspaceFormValidationSchema = yup.object().shape({
  name: yup.string().trim().required("form.empty.error"),
});

export const WorkspacesCreateControl: React.FC = () => {
  const { mutateAsync: createCloudWorkspace } = useCreateCloudWorkspace();
  const { formatMessage } = useIntl();
  const [isEditMode, toggleMode] = useToggle(false);
  const { registerNotification } = useNotificationService();
  const { workspaces } = useListCloudWorkspaces();
  const isFirstWorkspace = workspaces.length === 0;

  const onSubmit = async ({ name }: CreateWorkspaceFormValues) => {
    await createCloudWorkspace(name);
    toggleMode();
  };
  const onSuccess = () =>
    registerNotification({
      id: "workspaces.createSuccess",
      text: formatMessage({ id: "workspaces.createSuccess" }),
      type: "success",
    });

  const onError = (e: Error, { name }: CreateWorkspaceFormValues) => {
    trackError(e, { name });
    registerNotification({
      id: "workspaces.createError",
      text: formatMessage({ id: "workspaces.createError" }),
      type: "error",
    });
  };

  return (
    <Box mt="xl" mb="xl">
      {isEditMode ? (
        <Card withPadding>
          <Form<CreateWorkspaceFormValues>
            defaultValues={{
              name: "",
            }}
            schema={CreateWorkspaceFormValidationSchema}
            onSubmit={onSubmit}
            onSuccess={onSuccess}
            onError={onError}
          >
            <FormControl<CreateWorkspaceFormValues> label="Workspace name" name="name" fieldType="input" type="text" />
            <FormSubmissionButtons submitKey="form.saveChanges" onCancelClickCallback={toggleMode} />
          </Form>
        </Card>
      ) : (
        <Box mt="2xl">
          <Button onClick={toggleMode} data-testid="workspaces.createNew">
            <FormattedMessage id={isFirstWorkspace ? "workspaces.createFirst" : "workspaces.createNew"} />
          </Button>
        </Box>
      )}
    </Box>
  );
};
