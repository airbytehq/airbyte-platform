import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";
import { z } from "zod";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { ModalBody, ModalFooter } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useCreateWorkspace, useListDataplaneGroups } from "core/api";
import { DataplaneGroupRead } from "core/api/types/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";
import { trackError } from "core/utils/datadog";
import { links } from "core/utils/links";
import { useModalService } from "hooks/services/Modal";
import { useNotificationService } from "hooks/services/Notification";
import { BrandingBadge } from "views/layout/SideBar/AirbyteHomeLink";

import styles from "./OrganizationWorkspacesCreateControl.module.scss";
import TeamsUpsellGraphic from "./TeamsUpsellGraphic";
import { useOrganizationsToCreateWorkspaces } from "./useOrganizationsToCreateWorkspaces";

const OrganizationCreateWorkspaceFormValidationSchema = z.object({
  name: z.string().trim().nonempty("form.empty.error"),
  organizationId: z.string().trim().nonempty("form.empty.error"),
  dataplaneGroupId: z.string().trim().nonempty("form.empty.error"),
});

type CreateWorkspaceFormValues = z.infer<typeof OrganizationCreateWorkspaceFormValidationSchema>;

export const OrganizationWorkspacesCreateControl: React.FC<{ disabled?: boolean }> = ({ disabled = false }) => {
  const { organizationsToCreateIn } = useOrganizationsToCreateWorkspaces();
  const dataplaneGroups = useListDataplaneGroups();
  const hasMultiWorkspaceUI = useFeature(FeatureItem.MultiWorkspaceUI);
  const { openModal } = useModalService();
  const { formatMessage } = useIntl();

  // if the user does not have create permissions in any organizations, do not show the control at all
  if (organizationsToCreateIn.length === 0) {
    return null;
  }

  const handleButtonClick = () => {
    if (hasMultiWorkspaceUI) {
      openModal({
        title: formatMessage({ id: "workspaces.create.title" }),
        content: ({ onCancel }) => <CreateWorkspaceModal dataplaneGroups={dataplaneGroups} onCancel={onCancel} />,
      });
    } else {
      openModal({
        title: null,
        content: () => <UnlockWorkspacesModalBody />,
        size: "xl",
      });
    }
  };

  return (
    <Box>
      <Button
        onClick={handleButtonClick}
        variant="primary"
        data-testid="workspaces.createNew"
        size="sm"
        icon={hasMultiWorkspaceUI ? "plus" : "lock"}
        className={styles.createButton}
        disabled={disabled}
      >
        <FormattedMessage id="workspaces.createNew" />
      </Button>
    </Box>
  );
};

export const CreateWorkspaceModal: React.FC<{ dataplaneGroups: DataplaneGroupRead[]; onCancel: () => void }> = ({
  dataplaneGroups,
  onCancel,
}) => {
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
        dataplaneGroupId: dataplaneGroups[0]?.dataplane_group_id || "",
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
          label={formatMessage({ id: "form.dataplane" })}
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

export const UnlockWorkspacesModalBody: React.FC = () => {
  const { formatMessage } = useIntl();
  return (
    <ModalBody className={styles.modalBody}>
      <FlexContainer direction="row" gap="none">
        <FlexContainer direction="column" gap="lg" className={styles.modalBody__content}>
          <BrandingBadge product="cloudForTeams" />
          <Text size="xl" bold>
            {formatMessage({ id: "workspaces.unlock.title" })}
          </Text>
          <Box>
            <Text size="lg" align="left">
              {formatMessage({ id: "workspaces.unlock.featuresTitle" })}
            </Text>
            <ul className={styles.modalBody__list}>
              <li>{formatMessage({ id: "workspaces.unlock.features.multipleWorkspaces" })}</li>
              <li>{formatMessage({ id: "workspaces.unlock.features.subHourSyncs" })}</li>
              <li>{formatMessage({ id: "workspaces.unlock.features.sso" })}</li>
              <li>{formatMessage({ id: "workspaces.unlock.features.rbac" })}</li>
              <li>{formatMessage({ id: "workspaces.unlock.features.mappers" })}</li>
            </ul>
          </Box>
          <Text size="lg" align="left">
            {formatMessage({ id: "workspaces.unlock.upgradeMessage" })}
          </Text>
          <ExternalLink href={links.contactSales} opensInNewTab variant="buttonPrimary">
            <Icon type="lock" size="sm" />
            <Box ml="sm">
              <FormattedMessage id="workspaces.unlock.button" />
            </Box>
          </ExternalLink>
        </FlexContainer>
        <Box className={styles.modalBody__imgWrapper}>
          <TeamsUpsellGraphic />
        </Box>
      </FlexContainer>
    </ModalBody>
  );
};
