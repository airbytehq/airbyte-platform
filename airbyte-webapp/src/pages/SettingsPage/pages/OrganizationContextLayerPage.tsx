import React from "react";

import { FlexContainer } from "components/ui/Flex";

import { ContextLayerPage } from "cloud/components/AgentsOptIn";

const OrganizationContextLayerPage: React.FC = () => {
  return (
    <FlexContainer direction="column" gap="xl">
      <ContextLayerPage />
    </FlexContainer>
  );
};

export default OrganizationContextLayerPage;
