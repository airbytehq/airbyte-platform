import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";
import { useToggle } from "react-use";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { Icon } from "components/ui/Icon";

import { useCreateCloudWorkspace, useListCloudWorkspaces } from "core/api/cloud";
import { trackError } from "core/utils/datadog";
import { useNotificationService } from "hooks/services/Notification";

import styles from "./CloudWorkspacesCreateControl.module.scss";
interface CreateCloudWorkspaceFormValues {
  name: string;
}

const CreateWorkspaceFormValidationSchema = yup.object().shape({
  name: yup.string().trim().required("form.empty.error"),
});

export const CloudWorkspacesCreateControl: React.FC = () => {
  const navigate = useNavigate();
  const { mutateAsync: createWorkspace } = useCreateCloudWorkspace();
  const { formatMessage } = useIntl();
  const [isEditMode, toggleMode] = useToggle(false);
  const { registerNotification } = useNotificationService();
  const { workspaces } = useListCloudWorkspaces();
  const isFirstWorkspace = workspaces.length === 0;

  const onSubmit = async ({ name }: CreateCloudWorkspaceFormValues) => {
    const newWorkspace = await createWorkspace(name);
    toggleMode();
    newWorkspace && navigate(`/workspaces/${newWorkspace.workspaceId}`);
  };

  const onSuccess = () => {
    registerNotification({
      id: "workspaces.createSuccess",
      text: formatMessage({ id: "workspaces.createSuccess" }),
      type: "success",
    });
  };

  const onError = (e: Error, { name }: CreateCloudWorkspaceFormValues) => {
    trackError(e, { name });
    registerNotification({
      id: "workspaces.createError",
      text: formatMessage({ id: "workspaces.createError" }),
      type: "error",
    });
  };

  return (
    <>
      {isEditMode ? (
        <Card withPadding className={styles.animate}>
          <Form<CreateCloudWorkspaceFormValues>
            defaultValues={{
              name: "",
            }}
            schema={CreateWorkspaceFormValidationSchema}
            onSubmit={onSubmit}
            onSuccess={onSuccess}
            onError={onError}
          >
            <FormControl<CreateCloudWorkspaceFormValues>
              label="Workspace name"
              name="name"
              fieldType="input"
              type="text"
            />
            <FormSubmissionButtons
              submitKey="form.saveChanges"
              onCancelClickCallback={toggleMode}
              allowNonDirtyCancel
            />
          </Form>
        </Card>
      ) : (
        <Box>
          <Button
            onClick={toggleMode}
            variant="secondary"
            data-testid="workspaces.createNew"
            size="lg"
            icon={<Icon type="plus" />}
            className={styles.createButton}
          >
            <FormattedMessage id={isFirstWorkspace ? "workspaces.createFirst" : "workspaces.createNew"} />
          </Button>
        </Box>
      )}
    </>
  );
};
