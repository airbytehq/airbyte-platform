import React from "react";

import { PageContainer } from "components/PageContainer";
import { ScrollableContainer } from "components/ScrollableContainer";
import { FlexContainer } from "components/ui/Flex";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import { DbtCloudTransformations } from "./DbtCloudTransformations";

export const ConnectionTransformationPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.CONNECTIONS_ITEM_TRANSFORMATION);

  return (
    <ScrollableContainer>
      <PageContainer centered>
        <FlexContainer direction="column" gap="lg">
          <DbtCloudTransformations />
        </FlexContainer>
      </PageContainer>
    </ScrollableContainer>
  );
};
