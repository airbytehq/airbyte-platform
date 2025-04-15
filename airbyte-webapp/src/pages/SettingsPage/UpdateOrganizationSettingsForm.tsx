import React, { useEffect } from "react";
import { useIntl } from "react-intl";
import { z } from "zod";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";

import { useCurrentWorkspace, useUpdateOrganization, useOrganization } from "core/api";
import { useFeature, FeatureItem } from "core/services/features";
import { useIntent } from "core/utils/rbac";
import { useNotificationService } from "hooks/services/Notification";

import { RegionsTable } from "./components/RegionsTable";
const ORGANIZATION_UPDATE_NOTIFICATION_ID = "organization-update-notification";

const organizationValidationSchema = z.object({
  organizationName: z.string().trim().nonempty("form.empty.error"),
  email: z.string().email("form.email.error"),
});

type OrganizationFormValues = z.infer<typeof organizationValidationSchema>;

export const UpdateOrganizationSettingsForm: React.FC = () => {
  const { organizationId } = useCurrentWorkspace();

  return <OrganizationSettingsForm organizationId={organizationId} />;
};

const OrganizationSettingsForm = ({ organizationId }: { organizationId: string }) => {
  const organization = useOrganization(organizationId);
  const { mutateAsync: updateOrganization } = useUpdateOrganization();
  const supportsRegionsTable = useFeature(FeatureItem.AllowChangeDataGeographies);

  const { formatMessage } = useIntl();
  const { registerNotification, unregisterNotificationById } = useNotificationService();
  const canUpdateOrganization = useIntent("UpdateOrganization", { organizationId });

  const onSubmit = async (values: OrganizationFormValues) => {
    await updateOrganization({
      organizationId,
      ...values,
    });
  };

  useEffect(
    () => () => {
      unregisterNotificationById(ORGANIZATION_UPDATE_NOTIFICATION_ID);
    },
    [unregisterNotificationById]
  );

  return (
    <Form<OrganizationFormValues>
      onSubmit={onSubmit}
      onSuccess={() => {
        registerNotification({
          id: ORGANIZATION_UPDATE_NOTIFICATION_ID,
          text: formatMessage({ id: "form.changesSaved" }),
          type: "success",
        });
      }}
      onError={() => {
        registerNotification({
          id: ORGANIZATION_UPDATE_NOTIFICATION_ID,
          text: formatMessage({ id: "form.someError" }),
          type: "error",
        });
      }}
      zodSchema={organizationValidationSchema}
      defaultValues={{ organizationName: organization.organizationName, email: organization.email }}
      disabled={!canUpdateOrganization}
    >
      <FormControl<OrganizationFormValues>
        label={formatMessage({ id: "settings.organizationSettings.organizationName" })}
        fieldType="input"
        name="organizationName"
      />
      <FormControl<OrganizationFormValues>
        label={formatMessage({ id: "settings.organizationSettings.email" })}
        fieldType="input"
        name="email"
        labelTooltip={formatMessage({ id: "settings.organizationSettings.email.description" })}
      />
      {supportsRegionsTable && <RegionsTable />}
      {canUpdateOrganization && <FormSubmissionButtons noCancel justify="flex-start" submitKey="form.saveChanges" />}
    </Form>
  );
};
