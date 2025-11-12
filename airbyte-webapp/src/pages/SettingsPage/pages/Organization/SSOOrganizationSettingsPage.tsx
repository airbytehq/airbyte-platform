import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useExperiment } from "hooks/services/Experiment";
import { DomainVerificationSection } from "pages/SettingsPage/components/DomainVerification";
import { UpdateSSOSettingsForm } from "pages/SettingsPage/UpdateSSOSettingsForm";

export const SSOOrganizationSettingsPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_ORGANIZATION_SSO);
  const isDomainVerificationEnabled = useExperiment("settings.domainVerification");

  return (
    <FlexContainer direction="column" gap="xl">
      <Heading as="h1" size="md">
        <FormattedMessage id="settings.organization.sso.title" />
      </Heading>
      {isDomainVerificationEnabled && <DomainVerificationSection />}
      <UpdateSSOSettingsForm />
    </FlexContainer>
  );
};
