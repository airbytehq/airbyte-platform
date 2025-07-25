import { useEffect } from "react";
import { useIntl } from "react-intl";
import { z } from "zod";

import { Form } from "components/forms";
import { TeamsFeaturesWarnModal } from "components/TeamsFeaturesWarnModal";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { HttpProblem, useSSOConfigManagement } from "core/api";
import { useFormatError } from "core/errors";
import { useIntent } from "core/utils/rbac";
import { useOrganizationSubscriptionStatus } from "core/utils/useOrganizationSubscriptionStatus";
import { useExperiment } from "hooks/services/Experiment";
import { useModalService } from "hooks/services/Modal";
import { useNotificationService } from "hooks/services/Notification";

import { SSOSettings } from "./components/SSOSettings";

export const ssoValidationSchema = z.object({
  companyIdentifier: z.string().trim(),
  clientId: z.string().trim(),
  clientSecret: z.string().trim(),
  discoveryUrl: z.string().trim(),
  emailDomain: z.string().trim(),
});

export type SSOFormValues = z.infer<typeof ssoValidationSchema>;

const SSO_UPDATE_NOTIFICATION_ID = "sso-update-notification";

export const UpdateSSOSettingsForm = () => {
  const { formatMessage } = useIntl();
  const { registerNotification, unregisterNotificationById } = useNotificationService();
  const formatError = useFormatError();
  const organizationId = useCurrentOrganizationId();
  const canUpdateOrganization = useIntent("UpdateOrganization", { organizationId });
  const { ssoConfig, createSsoConfig } = useSSOConfigManagement();
  const { isInTrial } = useOrganizationSubscriptionStatus();
  const showTeamsFeaturesWarnModal = useExperiment("entitlements.showTeamsFeaturesWarnModal");
  const { openModal } = useModalService();

  const onSubmit = async (values: SSOFormValues) => {
    // Check if we need to show Teams features warning modal for trial users
    if (isInTrial && showTeamsFeaturesWarnModal) {
      await openModal({
        title: null,
        content: ({ onComplete }) => <TeamsFeaturesWarnModal onContinue={() => onComplete("success")} />,
        preventCancel: true,
        size: "xl",
      });
    }

    await createSsoConfig(values);
  };

  useEffect(
    () => () => {
      unregisterNotificationById(SSO_UPDATE_NOTIFICATION_ID);
    },
    [unregisterNotificationById]
  );

  const onError = (e: unknown) => {
    registerNotification({
      id: SSO_UPDATE_NOTIFICATION_ID,
      text: HttpProblem.isInstanceOf(e) ? formatError(e) : formatMessage({ id: "form.someError" }),
      type: "error",
    });
  };

  const onSuccess = () => {
    registerNotification({
      id: SSO_UPDATE_NOTIFICATION_ID,
      text: formatMessage({ id: "form.changesSaved" }),
      type: "success",
    });
  };

  return (
    <Form<SSOFormValues>
      onSubmit={onSubmit}
      onSuccess={onSuccess}
      onError={onError}
      zodSchema={ssoValidationSchema}
      defaultValues={{
        companyIdentifier: ssoConfig?.companyIdentifier || "",
        clientId: ssoConfig?.clientId || "",
        clientSecret: ssoConfig?.clientSecret || "",
        discoveryUrl: "", // we don't get this from the API
        emailDomain: ssoConfig?.emailDomains?.[0] || "",
      }}
      disabled={!canUpdateOrganization}
      reinitializeDefaultValues
    >
      <SSOSettings />
    </Form>
  );
};
