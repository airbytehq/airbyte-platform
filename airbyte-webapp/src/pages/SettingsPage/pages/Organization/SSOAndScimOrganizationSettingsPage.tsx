import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useExperiment } from "core/services/Experiment";
import { CollapsibleSettingsCard } from "pages/SettingsPage/components/CollapsibleSettingsCard";
import { DomainVerificationSection } from "pages/SettingsPage/components/DomainVerification";
import { UpdateSSOSettingsForm } from "pages/SettingsPage/UpdateSSOSettingsForm";

export const SSOAndScimOrganizationSettingsPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_ORGANIZATION_SSO);
  const { formatMessage } = useIntl();
  const isDomainVerificationEnabled = useExperiment("settings.domainVerification");
  const isScimProvisioningEnabled = useExperiment("settings.scimProvisioning");

  return (
    <FlexContainer direction="column" gap="xl">
      <Heading as="h1" size="md">
        <FormattedMessage
          id={isScimProvisioningEnabled ? "settings.organization.ssoAndScim.title" : "settings.organization.sso.title"}
        />
      </Heading>
      {isDomainVerificationEnabled && <DomainVerificationSection />}
      <UpdateSSOSettingsForm />
      {isScimProvisioningEnabled && (
        <CollapsibleSettingsCard label={formatMessage({ id: "settings.organizationSettings.scim.label" })} />
      )}
    </FlexContainer>
  );
};
