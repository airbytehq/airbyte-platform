import React, { useEffect } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";
import { AnySchema } from "yup";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";

import { useCurrentWorkspace, useUpdateOrganization, useOrganization } from "core/api";
import { OrganizationUpdateRequestBody } from "core/api/types/AirbyteClient";
import { useIntent } from "core/utils/rbac";
import { useNotificationService } from "hooks/services/Notification";

const ORGANIZATION_UPDATE_NOTIFICATION_ID = "organization-update-notification";

const organizationValidationSchema = yup.object().shape<Record<keyof OrganizationFormValues, AnySchema>>({
  organizationName: yup.string().trim().required("form.empty.error"),
  email: yup.string().email("form.email.error").trim().required("form.empty.error"),
});

type OrganizationFormValues = Pick<OrganizationUpdateRequestBody, "organizationName" | "email">;

export const GeneralOrganizationSettingsPage: React.FC = () => {
  const { organizationId } = useCurrentWorkspace();

  return (
    <Card title={<FormattedMessage id="settings.generalSettings" />}>
      <Box p="xl">{organizationId && <OrganizationSettingsForm organizationId={organizationId} />}</Box>
    </Card>
  );
};

const OrganizationSettingsForm = ({ organizationId }: { organizationId: string }) => {
  const organization = useOrganization(organizationId);
  const { mutateAsync: updateOrganization } = useUpdateOrganization();

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
      schema={organizationValidationSchema}
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
      {canUpdateOrganization && <FormSubmissionButtons submitKey="form.saveChanges" />}
    </Form>
  );
};
