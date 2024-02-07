import React from "react";
import { FormattedMessage } from "react-intl";

import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { useExperiment } from "hooks/services/Experiment";

import { OrganizationAccessManagementSection } from "./pages/AccessManagementPage/OrganizationAccessManagementSection";
import { UpdateOrganizationSettingsForm } from "./UpdateOrganizationSettingsForm";

export const GeneralOrganizationSettingsPage: React.FC = () => {
  const updatedOrganizationsUI = useExperiment("settings.organizationsUpdates", false);

  return (
    <FlexContainer direction="column" gap="xl">
      <Heading as="h2" size="md">
        <FormattedMessage id="settings.generalSettings" />
      </Heading>
      <Card>
        <UpdateOrganizationSettingsForm />
      </Card>
      {updatedOrganizationsUI && (
        <Card>
          <OrganizationAccessManagementSection />
        </Card>
      )}
    </FlexContainer>
  );
};
