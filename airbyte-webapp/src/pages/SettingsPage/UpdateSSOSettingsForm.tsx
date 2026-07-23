import { useEffect } from "react";
import { useIntl } from "react-intl";
import { z } from "zod";

import { Form } from "components/ui/forms";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { HttpProblem, useSSOConfigManagement } from "core/api";
import { useFormatError } from "core/errors";
import { useExperiment } from "core/services/Experiment";
import { useNotificationService } from "core/services/Notification";
import { useIntent } from "core/utils/rbac";

import { SSOSettings } from "./components/SSOSettings";
import { SSOSettingsValidation } from "./components/SSOSettingsValidation";
import { createSSOTestManager } from "./components/ssoTestManager";

export const ssoValidationSchema = z.object({
  companyIdentifier: z.string().trim(),
  clientId: z.string().trim(),
  clientSecret: z.string().trim(),
  discoveryUrl: z.string().trim(),
  emailDomain: z.string().trim(),
});

export const ssoValidationSchemaV2 = z.object({
  companyIdentifier: z.string().trim().nonempty("form.empty.error"),
  clientId: z.string().trim().nonempty("form.empty.error"),
  clientSecret: z.string().trim().nonempty("form.empty.error"),
  discoveryUrl: z.string().trim().nonempty("form.empty.error"),
});

export type SSOFormValues = z.infer<typeof ssoValidationSchema>;
export type SSOFormValuesValidation = z.infer<typeof ssoValidationSchemaV2>;

const SSO_UPDATE_NOTIFICATION_ID = "sso-update-notification";

export const UpdateSSOSettingsForm = () => {
  const { formatMessage } = useIntl();
  const { registerNotification, unregisterNotificationById } = useNotificationService();
  const formatError = useFormatError();
  const organizationId = useCurrentOrganizationId();
  const canUpdateOrganization = useIntent("UpdateOrganization", { organizationId });
  const { ssoConfig, createSsoConfig } = useSSOConfigManagement();
  const isSSOConfigValidationEnabled = useExperiment("settings.ssoConfigValidation");

  const onSubmit = async (values: SSOFormValues | SSOFormValuesValidation) => {
    await createSsoConfig(values);

    // For validation flow, redirect to OAuth test after saving draft config
    if (isSSOConfigValidationEnabled && "companyIdentifier" in values) {
      const userManager = createSSOTestManager(values.companyIdentifier);

      // Use UserManager to initiate the signin flow with IDP hint
      await userManager.signinRedirect({
        extraQueryParams: {
          kc_idp_hint: "default",
          prompt: "login",
        },
      });
    }
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
    // For validation flow, we redirect to OAuth so no notification needed
    if (isSSOConfigValidationEnabled) {
      return;
    }

    registerNotification({
      id: SSO_UPDATE_NOTIFICATION_ID,
      text: formatMessage({ id: "form.changesSaved" }),
      type: "success",
    });
  };

  if (isSSOConfigValidationEnabled) {
    return (
      <Form<SSOFormValuesValidation>
        onSubmit={onSubmit}
        onSuccess={onSuccess}
        onError={onError}
        zodSchema={ssoValidationSchemaV2}
        defaultValues={{
          companyIdentifier: ssoConfig?.companyIdentifier || "",
          clientId: ssoConfig?.clientId || "",
          clientSecret: "", // always empty - user must re-enter
          discoveryUrl: "", // we don't get this from the API
        }}
        disabled={!canUpdateOrganization}
        reinitializeDefaultValues
      >
        <SSOSettingsValidation />
      </Form>
    );
  }

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
