import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useExperiment } from "core/services/Experiment";
import { DomainVerificationSection } from "pages/SettingsPage/components/DomainVerification";
import { ScimSettingsCard } from "pages/SettingsPage/components/ScimSettingsCard";
import { UpdateSSOSettingsForm } from "pages/SettingsPage/UpdateSSOSettingsForm";

export const SSOAndScimOrganizationSettingsPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_ORGANIZATION_SSO);
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
      {isScimProvisioningEnabled && <ScimSettingsCard />}
    </FlexContainer>
  );
};
