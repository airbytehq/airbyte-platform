import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { FeatureItem, useFeature } from "core/services/features";
import { useExperiment } from "hooks/services/Experiment";

import { OrganizationAccessManagementSection } from "./pages/AccessManagementPage/OrganizationAccessManagementSection";
import { UpdateOrganizationSettingsForm } from "./UpdateOrganizationSettingsForm";

export const GeneralOrganizationSettingsPage: React.FC = () => {
  const updatedOrganizationsUI = useExperiment("settings.organizationsUpdates", false);
  const isAccessManagementEnabled = useFeature(FeatureItem.RBAC);

  return (
    <FlexContainer direction="column" gap="xl">
      <Heading as="h2" size="md">
        <FormattedMessage id="settings.generalSettings" />
      </Heading>
      <Card>
        <Box p="xl">
          <UpdateOrganizationSettingsForm />
        </Box>
      </Card>
      {isAccessManagementEnabled && updatedOrganizationsUI && (
        <Card>
          <Box p="xl">
            <OrganizationAccessManagementSection />
          </Box>
        </Card>
      )}
    </FlexContainer>
  );
};
