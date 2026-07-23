import React from "react";

import { FlexContainer } from "components/ui/Flex";
import { PageContainer } from "components/ui/PageContainer";
import { ScrollParent } from "components/ui/ScrollParent";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import { DbtCloudTransformations } from "./DbtCloudTransformations";

export const ConnectionTransformationPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.CONNECTIONS_ITEM_TRANSFORMATION);

  return (
    <ScrollParent>
      <PageContainer centered>
        <FlexContainer direction="column" gap="lg">
          <DbtCloudTransformations />
        </FlexContainer>
      </PageContainer>
    </ScrollParent>
  );
};
