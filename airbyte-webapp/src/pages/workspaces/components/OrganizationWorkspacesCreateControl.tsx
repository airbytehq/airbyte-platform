import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";
import { z } from "zod";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { TeamsFeaturesWarnModal } from "components/TeamsFeaturesWarnModal";
import { Button } from "components/ui/Button";
import { ModalBody, ModalFooter } from "components/ui/Modal";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useCreateWorkspace, useListDataplaneGroups } from "core/api";
import { DataplaneGroupRead } from "core/api/types/AirbyteClient";
import { trackError } from "core/utils/datadog";
import { useOrganizationSubscriptionStatus } from "core/utils/useOrganizationSubscriptionStatus";
import { useExperiment } from "hooks/services/Experiment";
import { useModalService } from "hooks/services/Modal";
import { useNotificationService } from "hooks/services/Notification";

import { useOrganizationsToCreateWorkspaces } from "./useOrganizationsToCreateWorkspaces";

const OrganizationCreateWorkspaceFormValidationSchema = z.object({
  name: z.string().trim().nonempty("form.empty.error"),
  organizationId: z.string().trim().nonempty("form.empty.error"),
  dataplaneGroupId: z.string().trim().nonempty("form.empty.error"),
});

type CreateWorkspaceFormValues = z.infer<typeof OrganizationCreateWorkspaceFormValidationSchema>;

export const OrganizationWorkspacesCreateControl: React.FC<{
  disabled?: boolean;
  secondary?: boolean;
  onCreated?: () => void;
}> = ({ disabled = false, secondary = false, onCreated }) => {
  const { isInTrial } = useOrganizationSubscriptionStatus();
  const showTeamsFeaturesWarnModal = useExperiment("entitlements.showTeamsFeaturesWarnModal");
  const { organizationsToCreateIn } = useOrganizationsToCreateWorkspaces();
  const dataplaneGroups = useListDataplaneGroups();
  const { openModal } = useModalService();
  const { formatMessage } = useIntl();

  // if the user does not have create permissions in any organizations, do not show the control at all
  if (organizationsToCreateIn.length === 0) {
    return null;
  }

  const handleButtonClick = () => {
    const openCreateWorkspaceModal = () =>
      openModal({
        title: formatMessage({ id: "workspaces.create.title" }),
        content: ({ onCancel }) => (
          <CreateWorkspaceModal dataplaneGroups={dataplaneGroups} onCancel={onCancel} onCreated={onCreated} />
        ),
      });

    if (isInTrial && showTeamsFeaturesWarnModal) {
      openModal({
        title: null,
        content: () => <TeamsFeaturesWarnModal onContinue={openCreateWorkspaceModal} />,
        size: "xl",
      });
    } else {
      openCreateWorkspaceModal();
    }
  };

  return (
    <Button
      onClick={handleButtonClick}
      variant={secondary ? "secondary" : "primary"}
      data-testid="workspaces.createNew"
      size="sm"
      icon={isInTrial ? "lock" : "plus"}
      disabled={disabled}
    >
      <FormattedMessage id="workspaces.createNew" />
    </Button>
  );
};

export const CreateWorkspaceModal: React.FC<{
  dataplaneGroups: DataplaneGroupRead[];
  onCancel: () => void;
  onCreated?: () => void;
}> = ({ dataplaneGroups, onCancel, onCreated }) => {
  const { mutateAsync: createWorkspace } = useCreateWorkspace();
  const navigate = useNavigate();
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const organizationId = useCurrentOrganizationId();

  const onSubmit = async (values: CreateWorkspaceFormValues) => {
    const newWorkspace = await createWorkspace(values);
    navigate(`/workspaces/${newWorkspace.workspaceId}`);
  };

  const onSuccess = () => {
    registerNotification({
      id: "workspaces.createSuccess",
      text: formatMessage({ id: "workspaces.createSuccess" }),
      type: "success",
    });
    onCreated?.();
  };

  const onError = (e: Error, { name }: CreateWorkspaceFormValues) => {
    trackError(e, { name });
    registerNotification({
      id: "workspaces.createError",
      text: formatMessage({ id: "workspaces.createError" }),
      type: "error",
    });
  };

  return (
    <Form<CreateWorkspaceFormValues>
      defaultValues={{
        name: "",
        organizationId,
        dataplaneGroupId: dataplaneGroups.find((group) => group.name === "US")?.dataplane_group_id || "",
      }}
      zodSchema={OrganizationCreateWorkspaceFormValidationSchema}
      onSubmit={onSubmit}
      onSuccess={onSuccess}
      onError={onError}
    >
      <ModalBody>
        <FormControl<CreateWorkspaceFormValues>
          label={formatMessage({ id: "form.workspaceName" })}
          name="name"
          fieldType="input"
          type="text"
        />
        <FormControl<CreateWorkspaceFormValues>
          label={formatMessage({ id: "form.region" })}
          name="dataplaneGroupId"
          fieldType="dropdown"
          adaptiveWidth={false}
          options={dataplaneGroups.map((dataplaneGroup) => {
            return {
              value: dataplaneGroup.dataplane_group_id,
              label: dataplaneGroup.name,
            };
          })}
        />
      </ModalBody>
      <ModalFooter>
        <FormSubmissionButtons submitKey="form.createWorkspace" onCancelClickCallback={onCancel} allowNonDirtyCancel />
      </ModalFooter>
    </Form>
  );
};
