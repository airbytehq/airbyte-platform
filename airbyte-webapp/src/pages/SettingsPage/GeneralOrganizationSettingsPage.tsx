import React from "react";
import { FormattedMessage } from "react-intl";

import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { FeatureItem, useFeature } from "core/services/features";

import { OrganizationAccessManagementSection } from "./pages/AccessManagementPage/OrganizationAccessManagementSection";
import { UpdateOrganizationSettingsForm } from "./UpdateOrganizationSettingsForm";

export const GeneralOrganizationSettingsPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_ORGANIZATION);
  const isAccessManagementEnabled = useFeature(FeatureItem.RBAC);

  return (
    <FlexContainer direction="column" gap="xl">
      <Heading as="h2" size="md">
        <FormattedMessage id="settings.members" />
      </Heading>
      <Card>
        <UpdateOrganizationSettingsForm />
      </Card>
      {isAccessManagementEnabled && (
        <Card>
          <OrganizationAccessManagementSection />
        </Card>
      )}
    </FlexContainer>
  );
};
