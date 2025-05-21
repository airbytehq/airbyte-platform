import React, { useEffect, useMemo } from "react";
import { useIntl } from "react-intl";
import { z } from "zod";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Box } from "components/ui/Box";

import { useCurrentWorkspace, useUpdateOrganization, useOrganization } from "core/api";
import { useFeature, FeatureItem } from "core/services/features";
import { useIntent } from "core/utils/rbac";
import { useNotificationService } from "hooks/services/Notification";

import { RegionsTable } from "./components/RegionsTable";
import { ssoRefinementFunction, SSOSettings, ssoValidationSchema } from "./components/SSOSettings";

const ORGANIZATION_UPDATE_NOTIFICATION_ID = "organization-update-notification";

const baseOrganizationValidationSchema = z.object({
  organizationName: z.string().trim().nonempty("form.empty.error"),
  email: z.string().email("form.email.error"),
});

export type BaseOrganizationFormValues = z.infer<typeof baseOrganizationValidationSchema>;

export const UpdateOrganizationSettingsForm: React.FC = () => {
  const { organizationId } = useCurrentWorkspace();

  return <OrganizationSettingsForm organizationId={organizationId} />;
};

const OrganizationSettingsForm = ({ organizationId }: { organizationId: string }) => {
  const organization = useOrganization(organizationId);
  const { mutateAsync: updateOrganization } = useUpdateOrganization();
  const supportsRegionsTable = useFeature(FeatureItem.AllowChangeDataplanes);
  const supportsSSO = useFeature(FeatureItem.AllowUpdateSSOConfig);

  const { formatMessage } = useIntl();
  const { registerNotification, unregisterNotificationById } = useNotificationService();
  const canUpdateOrganization = useIntent("UpdateOrganization", { organizationId });

  const organizationValidationSchema = useMemo(() => {
    return supportsSSO
      ? baseOrganizationValidationSchema.extend(ssoValidationSchema.shape).superRefine(ssoRefinementFunction)
      : baseOrganizationValidationSchema;
  }, [supportsSSO]);

  type OrganizationFormValues = z.infer<typeof organizationValidationSchema>;

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
      defaultValues={{
        organizationName: organization.organizationName,
        email: organization.email,
        ...(supportsSSO && {
          // TODO: replace with actual values when API is ready
          emailDomain: "",
          clientId: "",
          clientSecret: "",
          discoveryEndpoint: "",
          subdomain: "",
        }),
      }}
      disabled={!canUpdateOrganization}
    >
      <FormControl<BaseOrganizationFormValues>
        label={formatMessage({ id: "settings.organizationSettings.organizationName" })}
        fieldType="input"
        name="organizationName"
      />
      <FormControl<BaseOrganizationFormValues>
        label={formatMessage({ id: "settings.organizationSettings.email" })}
        fieldType="input"
        name="email"
        labelTooltip={formatMessage({ id: "settings.organizationSettings.email.description" })}
      />
      {(supportsRegionsTable || supportsSSO) && (
        <Box mb="lg">
          {supportsRegionsTable && <RegionsTable />}
          {supportsSSO && <SSOSettings />}
        </Box>
      )}
      {canUpdateOrganization && <FormSubmissionButtons noCancel justify="flex-start" submitKey="form.saveChanges" />}
    </Form>
  );
};
