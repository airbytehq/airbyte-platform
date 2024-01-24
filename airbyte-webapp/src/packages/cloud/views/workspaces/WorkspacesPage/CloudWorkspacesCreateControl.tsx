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
import { Text } from "components/ui/Text";

import { useListWorkspaces } from "core/api";
import { useCreateCloudWorkspace } from "core/api/cloud";
import { OrganizationRead } from "core/api/types/AirbyteClient";
import { trackError } from "core/utils/datadog";
import { useNotificationService } from "hooks/services/Notification";
import { useOrganizationsToCreateWorkspaces } from "pages/workspaces/components/useOrganizationsToCreateWorkspaces";
import styles from "pages/workspaces/components/WorkspacesCreateControl.module.scss";

interface CreateCloudWorkspaceFormValues {
  name: string;
  organizationId?: string;
}

const generateValidationSchema = (organizationsCount: number) => {
  return yup.object().shape({
    name: yup.string().trim().required("form.empty.error"),
    ...(organizationsCount > 0
      ? { organizationId: yup.string().required("form.empty.error") }
      : { organizationId: yup.string().strip() }),
  });
};

export const CloudWorkspacesCreateControl: React.FC = () => {
  const { mutateAsync: createWorkspace } = useCreateCloudWorkspace();

  const navigate = useNavigate();
  const { formatMessage } = useIntl();
  const [isEditMode, toggleMode] = useToggle(false);
  const { registerNotification } = useNotificationService();
  const { workspaces } = useListWorkspaces();
  const { organizationsToCreateIn, hasOrganization } = useOrganizationsToCreateWorkspaces();

  const isFirstWorkspace = workspaces.length === 0;

  const onSubmit = async (values: CreateCloudWorkspaceFormValues) => {
    const newWorkspace = await createWorkspace(values);
    toggleMode();
    navigate(`/workspaces/${newWorkspace.workspaceId}`);
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

  // if user is in an organization, but does not have adequate permissions, do not permit workspace creation
  if (organizationsToCreateIn.length === 0 && hasOrganization) {
    return null;
  }

  return (
    <>
      {isEditMode ? (
        <Card withPadding className={styles.animate}>
          <Form<CreateCloudWorkspaceFormValues>
            defaultValues={{
              name: "",
              organizationId:
                organizationsToCreateIn.length > 0 ? organizationsToCreateIn[0].organizationId : undefined,
            }}
            schema={generateValidationSchema(organizationsToCreateIn.length)}
            onSubmit={onSubmit}
            onSuccess={onSuccess}
            onError={onError}
          >
            <CloudWorkspaceCreateFormContent organizations={organizationsToCreateIn} toggleMode={toggleMode} />
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

const CloudWorkspaceCreateFormContent: React.FC<{ organizations: OrganizationRead[]; toggleMode: () => void }> = ({
  organizations,
  toggleMode,
}) => {
  const { formatMessage } = useIntl();

  return (
    <>
      {organizations.length > 1 && (
        <FormControl<CreateCloudWorkspaceFormValues>
          label={formatMessage({ id: "form.organizationName" })}
          name="organizationId"
          fieldType="dropdown"
          options={organizations.map((organization) => {
            return {
              value: organization.organizationId,
              label: organization.organizationName,
            };
          })}
        />
      )}
      <>
        <FormControl<CreateCloudWorkspaceFormValues>
          label={formatMessage({ id: "form.workspaceName" })}
          name="name"
          fieldType="input"
          type="text"
        />
        {organizations.length === 1 && (
          <Box pb="md">
            <Text>
              <FormattedMessage
                id="workspaces.create.organizationDescription"
                values={{ organizationName: organizations[0].organizationName }}
              />
            </Text>
          </Box>
        )}
        <FormSubmissionButtons submitKey="form.saveChanges" onCancelClickCallback={toggleMode} allowNonDirtyCancel />
      </>
    </>
  );
};
