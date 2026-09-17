import React from "react";

import { FlexContainer } from "components/ui/Flex";

import { InstallMcpPage } from "cloud/components/AgentsOptIn";

const OrganizationInstallMcpPage: React.FC = () => {
  return (
    <FlexContainer direction="column" gap="xl">
      <InstallMcpPage />
    </FlexContainer>
  );
};

export default OrganizationInstallMcpPage;
