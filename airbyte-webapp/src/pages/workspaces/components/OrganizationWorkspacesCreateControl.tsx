import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";
import { z } from "zod";

import { Button } from "components/ui/Button";
import { Form, FormControl } from "components/ui/forms";
import { FormSubmissionButtons } from "components/ui/forms/FormSubmissionButtons";
import { Icon } from "components/ui/Icon";
import { ExternalLink, Link } from "components/ui/Link";
import { ModalBody, ModalFooter } from "components/ui/Modal";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { useCurrentOrganizationId, useOrganizationPlan } from "area/organization/utils";
import { PlanAvailability, PlanAvailabilityBadges } from "cloud/area/billing/components/PlanAvailabilityBadges";
import { useLinkToPlanPage } from "cloud/area/billing/utils/useLinkToPlanPage";
import { HttpProblem, useCreateWorkspace, useListDataplaneGroups } from "core/api";
import { DataplaneGroupRead } from "core/api/types/AirbyteClient";
import { useModalService } from "core/services/Modal";
import { useNotificationService } from "core/services/Notification";
import { trackError } from "core/utils/datadog";
import { links } from "core/utils/links";
import { useProFeaturesModal } from "core/utils/useProFeaturesModal";

import styles from "./OrganizationWorkspacesCreateControl.module.scss";

const OrganizationCreateWorkspaceFormValidationSchema = z.object({
  name: z.string().trim().nonempty("form.empty.error"),
  organizationId: z.string().trim().nonempty("form.empty.error"),
  dataplaneGroupId: z.string().trim().nonempty("form.empty.error"),
});

type CreateWorkspaceFormValues = z.infer<typeof OrganizationCreateWorkspaceFormValidationSchema>;

// Each list is what that plan adds over the one below it, so a Standard user can see both sections
// without any feature being repeated.
const plusUpgradeMessageIds = [
  "proFeatures.modal.features.sso",
  "proFeatures.modal.features.upTo3Workspaces",
  "proFeatures.modal.features.mappers",
];

const proUpgradeMessageIds = [
  "proFeatures.modal.features.unlimitedWorkspaces",
  "proFeatures.modal.features.rbac",
  "proFeatures.modal.features.multipleDataRegions",
  "proFeatures.modal.features.userGroupsAndScim",
];

const UpsellSection: React.FC<{
  plan: PlanAvailability;
  titleId: string;
  featureMessageIds: string[];
  cta: React.ReactNode;
}> = ({ plan, titleId, featureMessageIds, cta }) => (
  <div className={styles.upsell__section}>
    <PlanAvailabilityBadges plans={[plan]} />
    <Text as="div" size="md" bold>
      <FormattedMessage id={titleId} />
    </Text>
    <ul className={styles.upsell__features}>
      {featureMessageIds.map((messageId) => (
        <li key={messageId}>
          <FormattedMessage id={messageId} />
        </li>
      ))}
    </ul>
    {cta}
  </div>
);

const WorkspaceLimitUpsell: React.FC<{ limitInfo?: { currentCount: number; limit: number } }> = ({ limitInfo }) => {
  const { isPlusPlan } = useOrganizationPlan();
  const linkToPlanPage = useLinkToPlanPage();

  return (
    <div className={styles.upsell__body}>
      {limitInfo && (
        <Text as="div" size="sm" color="grey400" className={styles.upsell__count}>
          <FormattedMessage
            id="workspaces.limitReached"
            values={{ count: limitInfo.currentCount, limit: limitInfo.limit }}
          />
        </Text>
      )}
      {!isPlusPlan && (
        <UpsellSection
          plan="plus"
          titleId="workspaces.upsell.upgradeToPlus"
          featureMessageIds={plusUpgradeMessageIds}
          cta={
            <Link to={linkToPlanPage} className={styles.upsell__cta}>
              <FormattedMessage id="workspaces.upsell.viewPlans" />
              <Icon type="arrowRight" size="sm" />
            </Link>
          }
        />
      )}
      <UpsellSection
        plan="pro"
        titleId="workspaces.upsell.upgradeToPro"
        featureMessageIds={proUpgradeMessageIds}
        cta={
          <ExternalLink href={links.contactSales} opensInNewTab className={styles.upsell__cta}>
            <FormattedMessage id="proFeatures.modal.button.talkToSales" />
            <Icon type="arrowRight" size="sm" />
          </ExternalLink>
        }
      />
    </div>
  );
};

export const OrganizationWorkspacesCreateControl: React.FC<{
  disabled?: boolean;
  secondary?: boolean;
  onCreated?: () => void;
  limitInfo?: { currentCount: number; limit: number };
}> = ({ disabled = false, secondary = false, onCreated, limitInfo }) => {
  const dataplaneGroups = useListDataplaneGroups();
  const { openModal } = useModalService();
  const { formatMessage } = useIntl();
  const { showProFeatureModalIfNeeded } = useProFeaturesModal("workspaces");

  const handleButtonClick = async () => {
    const openCreateWorkspaceModal = () =>
      openModal({
        title: formatMessage({ id: "workspaces.create.title" }),
        content: ({ onCancel }) => (
          <CreateWorkspaceModal dataplaneGroups={dataplaneGroups} onCancel={onCancel} onCreated={onCreated} />
        ),
      });

    await showProFeatureModalIfNeeded();
    openCreateWorkspaceModal();
  };

  if (disabled) {
    return (
      <Tooltip
        theme="light"
        placement="bottom"
        className={styles.upsell}
        control={
          <Button variant={secondary ? "secondary" : "primary"} size="sm" icon="lock" disabled>
            <FormattedMessage id="workspaces.createNew" />
          </Button>
        }
      >
        <WorkspaceLimitUpsell limitInfo={limitInfo} />
      </Tooltip>
    );
  }

  return (
    <Button
      onClick={handleButtonClick}
      variant={secondary ? "secondary" : "primary"}
      data-testid="workspaces.createNew"
      size="xs"
      icon="plus"
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
      text:
        HttpProblem.isType(e, "error:workspace-limit-for-organization-reached") && e.i18nType === "exact"
          ? e.translate(formatMessage)
          : formatMessage({ id: "workspaces.createError" }),
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
          adaptiveWidth
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
