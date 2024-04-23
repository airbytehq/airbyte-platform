import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Separator } from "components/ui/Separator";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { FeatureItem, useFeature } from "core/services/features";

import { OrganizationAccessManagementSection } from "./pages/AccessManagementPage/OrganizationAccessManagementSection";
import { UpdateOrganizationSettingsForm } from "./UpdateOrganizationSettingsForm";

export const GeneralOrganizationSettingsPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_ORGANIZATION);
  const isAccessManagementEnabled = useFeature(FeatureItem.RBAC);
  const displayOrganizationUsers = useFeature(FeatureItem.DisplayOrganizationUsers);

  return (
    <FlexContainer direction="column" gap="xl">
      <Heading as="h1" size="md">
        <FormattedMessage id="settings.organization.general.title" />
      </Heading>
      <UpdateOrganizationSettingsForm />

      {isAccessManagementEnabled && displayOrganizationUsers && (
        <>
          <Separator />
          <OrganizationAccessManagementSection />
        </>
      )}
    </FlexContainer>
  );
};
